//! Ensures that `Pod`s are configured and running for each [`AirflowCluster`]
use crate::config::{self, PYTHON_IMPORTS};
use crate::rbac;
use crate::util::env_var_from_secret;

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_airflow_crd::{
    airflowdb::{AirflowDB, AirflowDBStatusCondition},
    build_recommended_labels, AirflowCluster, AirflowConfig, AirflowConfigOptions, AirflowRole,
    AirflowStorageConfig, AIRFLOW_CONFIG_FILENAME, APP_NAME, CONFIG_PATH, OPERATOR_NAME,
};
use stackable_operator::commons::product_image_selection::ResolvedProductImage;
use stackable_operator::{
    builder::{
        ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder,
        PodSecurityContextBuilder, SecretOperatorVolumeSourceBuilder, VolumeBuilder,
    },
    cluster_resources::ClusterResources,
    commons::{
        authentication::{AuthenticationClass, AuthenticationClassProvider},
        resources::{NoRuntimeLimits, Resources},
        secret_class::SecretClassVolumeScope,
        tls::{CaCert, TlsServerVerification, TlsVerification},
    },
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, EnvVar, Probe, Service, ServicePort, ServiceSpec, TCPSocketAction,
                Volume,
            },
        },
        apimachinery::pkg::{apis::meta::v1::LabelSelector, util::intstr::IntOrString},
    },
    kube::{
        runtime::{controller::Action, reflector::ObjectRef},
        Resource, ResourceExt,
    },
    labels::{role_group_selector_labels, role_selector_labels},
    logging::controller::ReconcilerError,
    product_config::{
        flask_app_config_writer, flask_app_config_writer::FlaskAppConfigWriterError,
        types::PropertyNameKind, ProductConfigManager,
    },
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    role_utils::RoleGroupRef,
};
use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use strum::{EnumDiscriminants, IntoEnumIterator, IntoStaticStr};

pub const AIRFLOW_CONTROLLER_NAME: &str = "airflowcluster";
pub const SECRETS_DIR: &str = "/stackable/secrets/";
pub const CERTS_DIR: &str = "/stackable/certificates/";
pub const DOCKER_IMAGE_BASE_NAME: &str = "airflow";

const METRICS_PORT_NAME: &str = "metrics";
const METRICS_PORT: i32 = 9102;

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,
    #[snafu(display("failed to retrieve airflow version"))]
    NoAirflowVersion,
    #[snafu(display("object defines no statsd exporter version"))]
    ObjectHasNoStatsdExporterVersion,
    #[snafu(display("object defines no airflow config role"))]
    NoAirflowRole,
    #[snafu(display("failed to apply global Service"))]
    ApplyRoleService {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to apply Service for {rolegroup}"))]
    ApplyRoleGroupService {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<AirflowCluster>,
    },
    #[snafu(display("failed to apply ConfigMap for {rolegroup}"))]
    ApplyRoleGroupConfig {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<AirflowCluster>,
    },
    #[snafu(display("failed to apply StatefulSet for {rolegroup}"))]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<AirflowCluster>,
    },
    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("Failed to transform configs"))]
    ProductConfigTransform {
        source: stackable_operator::product_config_utils::ConfigError,
    },
    #[snafu(display("failed to apply Airflow DB"))]
    CreateAirflowDBObject {
        source: stackable_airflow_crd::airflowdb::Error,
    },
    #[snafu(display("failed to apply Airflow DB"))]
    ApplyAirflowDB {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to retrieve Airflow DB"))]
    AirflowDBRetrieval {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to patch service account: {source}"))]
    ApplyServiceAccount {
        name: String,
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to patch role binding: {source}"))]
    ApplyRoleBinding {
        name: String,
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to retrieve AuthenticationClass {authentication_class}"))]
    AuthenticationClassRetrieval {
        source: stackable_operator::error::Error,
        authentication_class: ObjectRef<AuthenticationClass>,
    },
    #[snafu(display(
        "Airflow doesn't support the AuthenticationClass provider
    {authentication_class_provider} from AuthenticationClass {authentication_class}"
    ))]
    AuthenticationClassProviderNotSupported {
        authentication_class_provider: String,
        authentication_class: ObjectRef<AuthenticationClass>,
    },
    #[snafu(display("failed to build config file for {rolegroup}"))]
    BuildRoleGroupConfigFile {
        source: FlaskAppConfigWriterError,
        rolegroup: RoleGroupRef<AirflowCluster>,
    },
    #[snafu(display("failed to build ConfigMap for {rolegroup}"))]
    BuildRoleGroupConfig {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<AirflowCluster>,
    },
    #[snafu(display("Airflow db {airflow_db} initialization failed, not starting airflow"))]
    AirflowDBFailed { airflow_db: ObjectRef<AirflowDB> },
    #[snafu(display("failed to resolve and merge resource config for role and role group"))]
    FailedToResolveResourceConfig {
        source: stackable_airflow_crd::Error,
    },
    #[snafu(display("could not parse Airflow role [{role}]"))]
    UnidentifiedAirflowRole {
        source: strum::ParseError,
        role: String,
    },
    #[snafu(display("failed to create cluster resources"))]
    CreateClusterResources {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::error::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

pub async fn reconcile_airflow(airflow: Arc<AirflowCluster>, ctx: Arc<Ctx>) -> Result<Action> {
    tracing::info!("Starting reconcile");

    let client = &ctx.client;
    let resolved_product_image: ResolvedProductImage =
        airflow.spec.image.resolve(DOCKER_IMAGE_BASE_NAME);

    // ensure admin user has been set up on the airflow database
    let airflow_db = AirflowDB::for_airflow(&airflow, &resolved_product_image)
        .context(CreateAirflowDBObjectSnafu)?;
    client
        .apply_patch(AIRFLOW_CONTROLLER_NAME, &airflow_db, &airflow_db)
        .await
        .context(ApplyAirflowDBSnafu)?;

    let airflow_db = client
        .get::<AirflowDB>(
            &airflow.name_unchecked(),
            airflow
                .namespace()
                .as_deref()
                .context(ObjectHasNoNamespaceSnafu)?,
        )
        .await
        .context(AirflowDBRetrievalSnafu)?;

    tracing::debug!("{}", format!("Checking status: {:#?}", airflow_db.status));

    if let Some(ref status) = airflow_db.status {
        match status.condition {
            AirflowDBStatusCondition::Pending | AirflowDBStatusCondition::Initializing => {
                tracing::debug!(
                    "Waiting for AirflowDB initialization to complete, not starting Airflow yet"
                );
                return Ok(Action::await_change());
            }
            AirflowDBStatusCondition::Failed => {
                return AirflowDBFailedSnafu {
                    airflow_db: ObjectRef::from_obj(&airflow_db),
                }
                .fail();
            }
            AirflowDBStatusCondition::Ready => (), // Continue starting Airflow
        }
    } else {
        tracing::debug!("Waiting for AirflowDBStatus to be reported, not starting Airflow yet");
        return Ok(Action::await_change());
    }

    let mut roles = HashMap::new();

    for role in AirflowRole::iter() {
        if let Some(resolved_role) = airflow.get_role(role.clone()).clone() {
            roles.insert(
                role.to_string(),
                (
                    vec![
                        PropertyNameKind::Env,
                        PropertyNameKind::File(AIRFLOW_CONFIG_FILENAME.into()),
                    ],
                    resolved_role,
                ),
            );
        }
    }

    let role_config = transform_all_roles_to_config::<AirflowConfig>(&airflow, roles);
    let validated_role_config = validate_all_roles_and_groups_config(
        &resolved_product_image.product_version,
        &role_config.context(ProductConfigTransformSnafu)?,
        &ctx.product_config,
        false,
        false,
    )
    .context(InvalidProductConfigSnafu)?;

    let (rbac_sa, rbac_rolebinding) = rbac::build_rbac_resources(airflow.as_ref(), "airflow");
    client
        .apply_patch(AIRFLOW_CONTROLLER_NAME, &rbac_sa, &rbac_sa)
        .await
        .with_context(|_| ApplyServiceAccountSnafu {
            name: rbac_sa.name_unchecked(),
        })?;
    client
        .apply_patch(
            AIRFLOW_CONTROLLER_NAME,
            &rbac_rolebinding,
            &rbac_rolebinding,
        )
        .await
        .with_context(|_| ApplyRoleBindingSnafu {
            name: rbac_rolebinding.name_unchecked(),
        })?;

    let authentication_class = match &airflow.spec.authentication_config {
        Some(authentication_config) => match &authentication_config.authentication_class {
            Some(authentication_class) => Some(
                AuthenticationClass::resolve(client, authentication_class)
                    .await
                    .context(AuthenticationClassRetrievalSnafu {
                        authentication_class: ObjectRef::<AuthenticationClass>::new(
                            authentication_class,
                        ),
                    })?,
            ),
            None => None,
        },
        None => None,
    };

    let mut cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        AIRFLOW_CONTROLLER_NAME,
        &airflow.object_ref(&()),
    )
    .context(CreateClusterResourcesSnafu)?;

    for (role_name, role_config) in validated_role_config.iter() {
        // some roles will only run "internally" and do not need to be created as services
        if let Some(resolved_port) = role_port(role_name) {
            let role_service =
                build_role_service(&airflow, &resolved_product_image, role_name, resolved_port)?;
            cluster_resources
                .add(client, &role_service)
                .await
                .context(ApplyRoleServiceSnafu)?;
        }

        for (rolegroup_name, rolegroup_config) in role_config.iter() {
            let rolegroup = RoleGroupRef {
                cluster: ObjectRef::from_obj(&*airflow),
                role: role_name.into(),
                role_group: rolegroup_name.into(),
            };

            let airflow_role =
                AirflowRole::from_str(role_name).context(UnidentifiedAirflowRoleSnafu {
                    role: role_name.to_string(),
                })?;

            let resources = airflow
                .resolve_resource_config_for_role_and_rolegroup(&airflow_role, &rolegroup)
                .context(FailedToResolveResourceConfigSnafu)?;

            let rg_service =
                build_rolegroup_service(&airflow, &resolved_product_image, &rolegroup)?;
            cluster_resources.add(client, &rg_service).await.context(
                ApplyRoleGroupServiceSnafu {
                    rolegroup: rolegroup.clone(),
                },
            )?;

            let rg_configmap = build_rolegroup_config_map(
                &airflow,
                &resolved_product_image,
                &rolegroup,
                rolegroup_config,
                authentication_class.as_ref(),
            )?;
            cluster_resources
                .add(client, &rg_configmap)
                .await
                .with_context(|_| ApplyRoleGroupConfigSnafu {
                    rolegroup: rolegroup.clone(),
                })?;

            let rg_statefulset = build_server_rolegroup_statefulset(
                &airflow,
                &resolved_product_image,
                &rolegroup,
                rolegroup_config,
                authentication_class.as_ref(),
                &rbac_sa.name_unchecked(),
                &resources,
            )?;
            cluster_resources
                .add(client, &rg_statefulset)
                .await
                .context(ApplyRoleGroupStatefulSetSnafu {
                    rolegroup: rolegroup.clone(),
                })?;
        }
    }

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphanedResourcesSnafu)?;

    Ok(Action::await_change())
}

/// The server-role service is the primary endpoint that should be used by clients that do not perform internal load balancing,
/// including targets outside of the cluster.
fn build_role_service(
    airflow: &AirflowCluster,
    resolved_product_image: &ResolvedProductImage,
    role_name: &str,
    port: u16,
) -> Result<Service> {
    let role_svc_name = format!(
        "{}-{}",
        airflow
            .metadata
            .name
            .as_ref()
            .unwrap_or(&APP_NAME.to_string()),
        role_name
    );
    let ports = role_ports(port);

    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(airflow)
            .name(&role_svc_name)
            .ownerreference_from_resource(airflow, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(build_recommended_labels(
                airflow,
                AIRFLOW_CONTROLLER_NAME,
                &resolved_product_image.app_version_label,
                role_name,
                "global",
            ))
            .build(),
        spec: Some(ServiceSpec {
            ports: Some(ports),
            selector: Some(role_selector_labels(airflow, APP_NAME, role_name)),
            type_: Some("NodePort".to_string()),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

fn role_ports(port: u16) -> Vec<ServicePort> {
    vec![ServicePort {
        name: Some(APP_NAME.to_string()),
        port: port.into(),
        protocol: Some("TCP".to_string()),
        ..ServicePort::default()
    }]
}

fn role_port(role_name: &str) -> Option<u16> {
    AirflowRole::from_str(role_name).unwrap().get_http_port()
}

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the administrator
fn build_rolegroup_config_map(
    airflow: &AirflowCluster,
    resolved_product_image: &ResolvedProductImage,
    rolegroup: &RoleGroupRef<AirflowCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    authentication_class: Option<&AuthenticationClass>,
) -> Result<ConfigMap, Error> {
    let mut config = rolegroup_config
        .get(&PropertyNameKind::File(AIRFLOW_CONFIG_FILENAME.to_string()))
        .cloned()
        .unwrap_or_default();

    config::add_airflow_config(
        &mut config,
        airflow.spec.authentication_config.as_ref(),
        authentication_class,
    );

    let mut config_file = Vec::new();
    flask_app_config_writer::write::<AirflowConfigOptions, _, _>(
        &mut config_file,
        config.iter(),
        PYTHON_IMPORTS,
    )
    .with_context(|_| BuildRoleGroupConfigFileSnafu {
        rolegroup: rolegroup.clone(),
    })?;

    ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(airflow)
                .name(rolegroup.object_name())
                .ownerreference_from_resource(airflow, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(build_recommended_labels(
                    airflow,
                    AIRFLOW_CONTROLLER_NAME,
                    &resolved_product_image.app_version_label,
                    &rolegroup.role,
                    &rolegroup.role_group,
                ))
                .build(),
        )
        .add_data(
            AIRFLOW_CONFIG_FILENAME,
            String::from_utf8(config_file).unwrap(),
        )
        .build()
        .with_context(|_| BuildRoleGroupConfigSnafu {
            rolegroup: rolegroup.clone(),
        })
}

/// The rolegroup [`Service`] is a headless service that allows direct access to the instances of a certain rolegroup
///
/// This is mostly useful for internal communication between peers, or for clients that perform client-side load balancing.
fn build_rolegroup_service(
    airflow: &AirflowCluster,
    resolved_product_image: &ResolvedProductImage,
    rolegroup: &RoleGroupRef<AirflowCluster>,
) -> Result<Service> {
    let mut ports = vec![ServicePort {
        name: Some(METRICS_PORT_NAME.into()),
        port: METRICS_PORT,
        protocol: Some("TCP".to_string()),
        ..Default::default()
    }];

    if let Some(http_port) = role_port(&rolegroup.role) {
        ports.append(&mut role_ports(http_port));
    }

    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(airflow)
            .name(&rolegroup.object_name())
            .ownerreference_from_resource(airflow, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(build_recommended_labels(
                airflow,
                AIRFLOW_CONTROLLER_NAME,
                &resolved_product_image.app_version_label,
                &rolegroup.role,
                &rolegroup.role_group,
            ))
            .with_label("prometheus.io/scrape", "true")
            .build(),
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            ports: Some(ports),
            selector: Some(role_group_selector_labels(
                airflow,
                APP_NAME,
                &rolegroup.role,
                &rolegroup.role_group,
            )),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

/// The rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
///
/// The [`Pod`](`stackable_operator::k8s_openapi::api::core::v1::Pod`)s are accessible through the corresponding [`Service`] (from [`build_rolegroup_service`]).
fn build_server_rolegroup_statefulset(
    airflow: &AirflowCluster,
    resolved_product_image: &ResolvedProductImage,
    rolegroup_ref: &RoleGroupRef<AirflowCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    authentication_class: Option<&AuthenticationClass>,
    sa_name: &str,
    resources: &Resources<AirflowStorageConfig, NoRuntimeLimits>,
) -> Result<StatefulSet> {
    let airflow_role = AirflowRole::from_str(&rolegroup_ref.role).unwrap();
    let role = airflow
        .get_role(airflow_role.clone())
        .as_ref()
        .context(NoAirflowRoleSnafu)?;

    let rolegroup = role.role_groups.get(&rolegroup_ref.role_group);

    // initialising commands
    let commands = airflow_role.get_commands();

    // container
    let mut cb = ContainerBuilder::new(APP_NAME).expect("ContainerBuilder not created");
    let mut pb = PodBuilder::new();

    if let Some(authentication_class) = authentication_class {
        add_authentication_volumes_and_volume_mounts(authentication_class, &mut cb, &mut pb)?;
    }

    let cb = cb
        .image_from_product_image(resolved_product_image)
        .resources(resources.clone().into())
        .command(vec!["/bin/bash".to_string()])
        .args(vec![String::from("-c"), commands.join("; ")]);

    // environment variables
    let env_config = rolegroup_config
        .get(&PropertyNameKind::Env)
        .iter()
        .flat_map(|env_vars| env_vars.iter())
        .map(|(k, v)| EnvVar {
            name: k.clone(),
            value: Some(v.clone()),
            ..EnvVar::default()
        })
        .collect::<Vec<_>>();

    // mapped environment variables
    let env_mapped = build_mapped_envs(airflow, rolegroup_config);

    cb.add_env_vars(env_config);
    cb.add_env_vars(env_mapped);
    cb.add_env_vars(build_static_envs());

    let volume_mounts = airflow.volume_mounts();
    cb.add_volume_mounts(volume_mounts);
    cb.add_volume_mount("config", CONFIG_PATH);

    if let Some(resolved_port) = airflow_role.get_http_port() {
        let probe = Probe {
            tcp_socket: Some(TCPSocketAction {
                port: IntOrString::Int(resolved_port.into()),
                ..TCPSocketAction::default()
            }),
            initial_delay_seconds: Some(20),
            period_seconds: Some(5),
            ..Probe::default()
        };
        cb.readiness_probe(probe.clone());
        cb.liveness_probe(probe);
        cb.add_container_port("http", resolved_port.into());
    }

    let container = cb.build();

    let metrics_container = ContainerBuilder::new("metrics")
        .expect("ContainerBuilder not created")
        .image_from_product_image(resolved_product_image)
        .command(vec!["/bin/bash".to_string(), "-c".to_string()])
        .args(vec!["/stackable/statsd_exporter".to_string()])
        .add_container_port(METRICS_PORT_NAME, METRICS_PORT)
        .build();

    let mut volumes = airflow.volumes();

    volumes.push(
        VolumeBuilder::new("config")
            .with_config_map(rolegroup_ref.object_name())
            .build(),
    );

    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(airflow)
            .name(&rolegroup_ref.object_name())
            .ownerreference_from_resource(airflow, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(build_recommended_labels(
                airflow,
                AIRFLOW_CONTROLLER_NAME,
                &resolved_product_image.app_version_label,
                &rolegroup_ref.role,
                &rolegroup_ref.role_group,
            ))
            .with_label("restarter.stackable.tech/enabled", "true")
            .build(),
        spec: Some(StatefulSetSpec {
            pod_management_policy: Some("Parallel".to_string()),
            replicas: if airflow.spec.stopped.unwrap_or(false) {
                Some(0)
            } else {
                rolegroup.and_then(|rg| rg.replicas).map(i32::from)
            },
            selector: LabelSelector {
                match_labels: Some(role_group_selector_labels(
                    airflow,
                    APP_NAME,
                    &rolegroup_ref.role,
                    &rolegroup_ref.role_group,
                )),
                ..LabelSelector::default()
            },
            service_name: rolegroup_ref.object_name(),
            template: pb
                .metadata_builder(|m| {
                    m.with_recommended_labels(build_recommended_labels(
                        airflow,
                        AIRFLOW_CONTROLLER_NAME,
                        &resolved_product_image.app_version_label,
                        &rolegroup_ref.role,
                        &rolegroup_ref.role_group,
                    ))
                })
                .image_pull_secrets_from_product_image(resolved_product_image)
                .add_container(container)
                .add_container(metrics_container)
                .add_volumes(volumes)
                .node_selector_opt(rolegroup.and_then(|rg| rg.selector.clone()))
                .service_account_name(sa_name)
                .security_context(
                    PodSecurityContextBuilder::new()
                        .run_as_user(rbac::AIRFLOW_UID)
                        .run_as_group(0)
                        .fs_group(1000) // Needed for secret-operator
                        .build(),
                )
                .build_template(),
            ..StatefulSetSpec::default()
        }),
        status: None,
    })
}

/// This builds a collection of environment variables some require some minimal mapping,
/// such as executor type, contents of the secret etc.
fn build_mapped_envs(
    airflow: &AirflowCluster,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Vec<EnvVar> {
    let secret_prop = rolegroup_config
        .get(&PropertyNameKind::Env)
        .and_then(|vars| vars.get(AirflowConfig::CREDENTIALS_SECRET_PROPERTY));

    let mut env = secret_prop
        .map(|secret| {
            vec![
                // The secret key is used to run the webserver flask app and also used to authorize
                // requests to Celery workers when logs are retrieved.
                env_var_from_secret(
                    "AIRFLOW__WEBSERVER__SECRET_KEY",
                    secret,
                    "connections.secretKey",
                ),
                env_var_from_secret(
                    "AIRFLOW__CORE__SQL_ALCHEMY_CONN",
                    secret,
                    "connections.sqlalchemyDatabaseUri",
                ),
                env_var_from_secret(
                    "AIRFLOW__CELERY__RESULT_BACKEND",
                    secret,
                    "connections.celeryResultBackend",
                ),
                env_var_from_secret(
                    "AIRFLOW__CELERY__BROKER_URL",
                    secret,
                    "connections.celeryBrokerUrl",
                ),
            ]
        })
        .unwrap_or_default();

    if let Some(true) = airflow.spec.load_examples {
        env.push(EnvVar {
            name: "AIRFLOW__CORE__LOAD_EXAMPLES".into(),
            value: Some("True".into()),
            ..Default::default()
        })
    } else {
        env.push(EnvVar {
            name: "AIRFLOW__CORE__LOAD_EXAMPLES".into(),
            value: Some("False".into()),
            ..Default::default()
        })
    }

    if let Some(true) = airflow.spec.expose_config {
        env.push(EnvVar {
            name: "AIRFLOW__WEBSERVER__EXPOSE_CONFIG".into(),
            value: Some("True".into()),
            ..Default::default()
        })
    }

    let executor = airflow.spec.executor.clone();

    env.push(EnvVar {
        name: "AIRFLOW__CORE__EXECUTOR".into(),
        value: executor,
        ..Default::default()
    });

    env
}

fn build_static_envs() -> Vec<EnvVar> {
    [
        EnvVar {
            name: "AIRFLOW__METRICS__STATSD_ON".into(),
            value: Some("True".into()),
            ..Default::default()
        },
        EnvVar {
            name: "AIRFLOW__METRICS__STATSD_HOST".into(),
            value: Some("0.0.0.0".into()),
            ..Default::default()
        },
        EnvVar {
            name: "AIRFLOW__METRICS__STATSD_PORT".into(),
            value: Some("9125".into()),
            ..Default::default()
        },
        // Authentication for the API is handled separately to the Web Authentication.
        // Basic authentication is used by the integration tests.
        // The default is to deny all requests to the API.
        EnvVar {
            name: "AIRFLOW__API__AUTH_BACKEND".into(),
            value: Some("airflow.api.auth.backend.basic_auth".into()),
            ..Default::default()
        },
    ]
    .into()
}

pub fn error_policy(_obj: Arc<AirflowCluster>, _error: &Error, _ctx: Arc<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(5))
}

fn add_authentication_volumes_and_volume_mounts(
    authentication_class: &AuthenticationClass,
    cb: &mut ContainerBuilder,
    pb: &mut PodBuilder,
) -> Result<()> {
    let authentication_class_name = authentication_class.metadata.name.as_ref().unwrap();

    match &authentication_class.spec.provider {
        AuthenticationClassProvider::Ldap(ldap) => {
            if let Some(bind_credentials) = &ldap.bind_credentials {
                let volume_name = format!("{authentication_class_name}-bind-credentials");

                pb.add_volume(build_secret_operator_volume(
                    &volume_name,
                    &bind_credentials.secret_class,
                    bind_credentials.scope.as_ref(),
                ));
                cb.add_volume_mount(&volume_name, format!("{SECRETS_DIR}{volume_name}"));
            }

            if let Some(tls) = &ldap.tls {
                match &tls.verification {
                    TlsVerification::Server(TlsServerVerification {
                        ca_cert: CaCert::SecretClass(cert_secret_class),
                    }) => {
                        let volume_name = format!("{authentication_class_name}-tls-certificate");

                        pb.add_volume(build_secret_operator_volume(
                            &volume_name,
                            cert_secret_class,
                            None,
                        ));
                        cb.add_volume_mount(&volume_name, format!("{CERTS_DIR}{volume_name}"));
                    }
                    // Explicitly listing other possibilities to not oversee new enum variants in the future
                    TlsVerification::None {}
                    | TlsVerification::Server(TlsServerVerification {
                        ca_cert: CaCert::WebPki {},
                    }) => {}
                }
            }

            Ok(())
        }
        _ => AuthenticationClassProviderNotSupportedSnafu {
            authentication_class_provider: authentication_class.spec.provider.to_string(),
            authentication_class: ObjectRef::<AuthenticationClass>::new(authentication_class_name),
        }
        .fail(),
    }
}

fn build_secret_operator_volume(
    volume_name: &str,
    secret_class_name: &str,
    scope: Option<&SecretClassVolumeScope>,
) -> Volume {
    let mut secret_operator_volume_source_builder =
        SecretOperatorVolumeSourceBuilder::new(secret_class_name);

    if let Some(scope) = scope {
        if scope.pod {
            secret_operator_volume_source_builder.with_pod_scope();
        }
        if scope.node {
            secret_operator_volume_source_builder.with_node_scope();
        }
        for service in &scope.services {
            secret_operator_volume_source_builder.with_service_scope(service);
        }
    }

    VolumeBuilder::new(volume_name)
        .ephemeral(secret_operator_volume_source_builder.build())
        .build()
}
