//! Ensures that `Pod`s are configured and running for each [`AirflowCluster`]
use stackable_operator::builder::resources::ResourceRequirementsBuilder;
use stackable_operator::config::fragment::ValidationError;
use stackable_operator::k8s_openapi::DeepMerge;
use stackable_operator::product_logging::framework::{
    capture_shell_output, shutdown_vector_command,
};
use stackable_operator::product_logging::spec::{ContainerLogConfig, ContainerLogConfigChoice};

use crate::config::{self, PYTHON_IMPORTS};
use crate::controller_commons::{
    self, CONFIG_VOLUME_NAME, LOG_CONFIG_VOLUME_NAME, LOG_VOLUME_NAME,
};
use crate::product_logging::{
    extend_config_map_with_log_config, resolve_vector_aggregator_address,
};
use crate::util::env_var_from_secret;

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_airflow_crd::airflowdb::AirflowDBStatus;
use stackable_airflow_crd::authentication::AirflowAuthenticationConfigResolved;
use stackable_airflow_crd::{
    airflowdb::{AirflowDB, AirflowDBStatusCondition},
    build_recommended_labels, AirflowCluster, AirflowConfig, AirflowConfigFragment,
    AirflowConfigOptions, AirflowRole, Container, AIRFLOW_CONFIG_FILENAME, APP_NAME, CONFIG_PATH,
    LOG_CONFIG_DIR, OPERATOR_NAME, STACKABLE_LOG_DIR,
};
use stackable_airflow_crd::{
    AirflowClusterStatus, AirflowExecutor, ExecutorConfig, AIRFLOW_UID, GIT_CONTENT, GIT_LINK,
    GIT_ROOT, GIT_SYNC_DIR, GIT_SYNC_NAME, TEMPLATE_CONFIGMAP_NAME, TEMPLATE_LOCATION,
    TEMPLATE_NAME, TEMPLATE_VOLUME_NAME,
};
use stackable_operator::{
    builder::{
        ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder,
        PodSecurityContextBuilder, VolumeBuilder,
    },
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::{
        authentication::{AuthenticationClass, AuthenticationClassProvider},
        product_image_selection::ResolvedProductImage,
        rbac::build_rbac_resources,
    },
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, EmptyDirVolumeSource, EnvVar, Probe, Service, ServicePort, ServiceSpec,
                TCPSocketAction,
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
    product_logging::{self, spec::Logging},
    role_utils::RoleGroupRef,
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder, ClusterCondition, ClusterConditionSet,
        ClusterConditionStatus, ClusterConditionType, ConditionBuilder,
    },
};
use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use strum::{EnumDiscriminants, IntoEnumIterator, IntoStaticStr};

pub const AIRFLOW_CONTROLLER_NAME: &str = "airflowcluster";
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
    #[snafu(display("failed to patch service account"))]
    ApplyServiceAccount {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to patch role binding: {source}"))]
    ApplyRoleBinding {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to build RBAC objects"))]
    BuildRBACObjects {
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
    #[snafu(display("failed to resolve and merge config for role and role group"))]
    FailedToResolveConfig {
        source: stackable_airflow_crd::Error,
    },
    #[snafu(display("could not parse Airflow role [{role}]"))]
    UnidentifiedAirflowRole {
        source: strum::ParseError,
        role: String,
    },
    #[snafu(display("invalid executor name"))]
    UnidentifiedAirflowExecutor {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("invalid container name"))]
    InvalidContainerName {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to create cluster resources"))]
    CreateClusterResources {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to resolve the Vector aggregator address"))]
    ResolveVectorAggregatorAddress {
        source: crate::product_logging::Error,
    },
    #[snafu(display("failed to add the logging configuration to the ConfigMap [{cm_name}]"))]
    InvalidLoggingConfig {
        source: crate::product_logging::Error,
        cm_name: String,
    },
    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to apply authentication configuration"))]
    InvalidAuthenticationConfig {
        source: stackable_airflow_crd::authentication::Error,
    },
    #[snafu(display("pod template serialization"))]
    PodTemplateSerde { source: serde_yaml::Error },
    #[snafu(display("failed to build the pod template config map"))]
    PodTemplateConfigMap {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to apply executor template ConfigMap"))]
    ApplyExecutorTemplateConfig {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("fragment validation failure"))]
    FragmentValidationFailure { source: ValidationError },
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
    let resolved_product_image: ResolvedProductImage = airflow
        .spec
        .image
        .resolve(DOCKER_IMAGE_BASE_NAME, crate::built_info::CARGO_PKG_VERSION);

    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&airflow.spec.cluster_operation);

    if wait_for_db_and_update_status(
        client,
        &airflow,
        &resolved_product_image,
        &cluster_operation_cond_builder,
    )
    .await?
    {
        return Ok(Action::await_change());
    }

    let authentication_config = airflow
        .spec
        .cluster_config
        .authentication
        .resolve(client)
        .await
        .context(InvalidAuthenticationConfigSnafu)?;

    let mut roles = HashMap::new();

    // if the kubernetes executor is specified there will be no worker role as the pods
    // are provisioned by airflow as defined by the task (default: one pod per task)
    for role in AirflowRole::iter() {
        if let Some(resolved_role) = airflow.get_role(&role) {
            roles.insert(
                role.to_string(),
                (
                    vec![
                        PropertyNameKind::Env,
                        PropertyNameKind::File(AIRFLOW_CONFIG_FILENAME.into()),
                    ],
                    resolved_role.clone(),
                ),
            );
        }
    }

    let role_config = transform_all_roles_to_config::<AirflowConfigFragment>(&airflow, roles);
    let validated_role_config = validate_all_roles_and_groups_config(
        &resolved_product_image.product_version,
        &role_config.context(ProductConfigTransformSnafu)?,
        &ctx.product_config,
        false,
        false,
    )
    .context(InvalidProductConfigSnafu)?;

    let vector_aggregator_address = resolve_vector_aggregator_address(
        client,
        airflow.as_ref(),
        airflow
            .spec
            .cluster_config
            .vector_aggregator_config_map_name
            .as_deref(),
    )
    .await
    .context(ResolveVectorAggregatorAddressSnafu)?;

    let mut cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        AIRFLOW_CONTROLLER_NAME,
        &airflow.object_ref(&()),
        ClusterResourceApplyStrategy::from(&airflow.spec.cluster_operation),
    )
    .context(CreateClusterResourcesSnafu)?;

    let (rbac_sa, rbac_rolebinding) = build_rbac_resources(
        airflow.as_ref(),
        APP_NAME,
        cluster_resources.get_required_labels(),
    )
    .context(BuildRBACObjectsSnafu)?;

    let rbac_sa = cluster_resources
        .add(client, rbac_sa)
        .await
        .context(ApplyServiceAccountSnafu)?;
    cluster_resources
        .add(client, rbac_rolebinding)
        .await
        .context(ApplyRoleBindingSnafu)?;

    let mut ss_cond_builder = StatefulSetConditionBuilder::default();

    let airflow_executor = &airflow.spec.executor;

    // if the kubernetes executor is specified, in place of a worker role that will be in the role
    // collection there will be a pod template created to be used for pod provisioning
    if let AirflowExecutor::KubernetesExecutor {
        common_configuration,
    } = &airflow_executor
    {
        build_executor_template(
            &airflow,
            &common_configuration.config,
            &resolved_product_image,
            &authentication_config,
            &vector_aggregator_address,
            &mut cluster_resources,
            client,
            &rbac_sa,
            &common_configuration.env_overrides,
        )
        .await?;
    }

    // from this point on we only need the discriminant
    //let airflow_executor = AirflowExecutorDiscriminants::from(airflow_executor);

    for (role_name, role_config) in validated_role_config.iter() {
        // some roles will only run "internally" and do not need to be created as services
        if let Some(resolved_port) = role_port(role_name) {
            let role_service =
                build_role_service(&airflow, &resolved_product_image, role_name, resolved_port)?;
            cluster_resources
                .add(client, role_service)
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

            let config = airflow
                .merged_config(&airflow_role, &rolegroup)
                .context(FailedToResolveConfigSnafu)?;

            let rg_service =
                build_rolegroup_service(&airflow, &resolved_product_image, &rolegroup)?;
            cluster_resources.add(client, rg_service).await.context(
                ApplyRoleGroupServiceSnafu {
                    rolegroup: rolegroup.clone(),
                },
            )?;

            let rg_statefulset = build_server_rolegroup_statefulset(
                &airflow,
                &resolved_product_image,
                &airflow_role,
                &rolegroup,
                rolegroup_config,
                authentication_config.as_ref(),
                &rbac_sa.name_unchecked(),
                &config,
                airflow_executor,
            )?;

            ss_cond_builder.add(
                cluster_resources
                    .add(client, rg_statefulset)
                    .await
                    .context(ApplyRoleGroupStatefulSetSnafu {
                        rolegroup: rolegroup.clone(),
                    })?,
            );

            let rg_configmap = build_rolegroup_config_map(
                &airflow,
                &resolved_product_image,
                &rolegroup,
                rolegroup_config,
                authentication_config.as_ref(),
                &config.logging,
                vector_aggregator_address.as_deref(),
                &Container::Airflow,
            )?;
            cluster_resources
                .add(client, rg_configmap)
                .await
                .with_context(|_| ApplyRoleGroupConfigSnafu {
                    rolegroup: rolegroup.clone(),
                })?;
        }
    }

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphanedResourcesSnafu)?;

    let status = AirflowClusterStatus {
        conditions: compute_conditions(
            airflow.as_ref(),
            &[&ss_cond_builder, &cluster_operation_cond_builder],
        ),
    };

    client
        .apply_patch_status(OPERATOR_NAME, &*airflow, &status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(Action::await_change())
}

#[allow(clippy::too_many_arguments)]
async fn build_executor_template(
    airflow: &Arc<AirflowCluster>,
    config: &stackable_airflow_crd::ExecutorConfigFragment,
    resolved_product_image: &ResolvedProductImage,
    authentication_config: &Vec<AirflowAuthenticationConfigResolved>,
    vector_aggregator_address: &Option<String>,
    cluster_resources: &mut ClusterResources,
    client: &stackable_operator::client::Client,
    rbac_sa: &stackable_operator::k8s_openapi::api::core::v1::ServiceAccount,
    env_overrides: &HashMap<String, String>,
) -> Result<(), Error> {
    let config = airflow
        .merged_executor_config(config)
        .context(FailedToResolveConfigSnafu)?;
    let rolegroup = RoleGroupRef {
        cluster: ObjectRef::from_obj(&**airflow),
        role: "executor".into(),
        role_group: "kubernetes".into(),
    };
    let rg_configmap = build_rolegroup_config_map(
        airflow,
        resolved_product_image,
        &rolegroup,
        &HashMap::new(),
        authentication_config,
        &config.logging,
        vector_aggregator_address.as_deref(),
        &Container::Base,
    )?;
    cluster_resources
        .add(client, rg_configmap)
        .await
        .with_context(|_| ApplyRoleGroupConfigSnafu {
            rolegroup: rolegroup.clone(),
        })?;
    let worker_pod_template_config_map = build_executor_template_config_map(
        airflow,
        resolved_product_image,
        authentication_config,
        &rbac_sa.name_unchecked(),
        &config,
        env_overrides,
        &rolegroup,
    )?;
    cluster_resources
        .add(client, worker_pod_template_config_map)
        .await
        .with_context(|_| ApplyExecutorTemplateConfigSnafu {})?;
    Ok(())
}

/// The server-role service is the primary endpoint that should be used by clients that do not perform internal load balancing,
/// including targets outside of the cluster.
fn build_role_service(
    airflow: &AirflowCluster,
    resolved_product_image: &ResolvedProductImage,
    role_name: &str,
    port: u16,
) -> Result<Service> {
    let role_svc_name = format!("{}-{}", airflow.name_any(), role_name);
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
            type_: Some(
                airflow
                    .spec
                    .cluster_config
                    .listener_class
                    .k8s_service_type(),
            ),
            ports: Some(ports),
            selector: Some(role_selector_labels(airflow, APP_NAME, role_name)),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

fn role_ports(port: u16) -> Vec<ServicePort> {
    vec![ServicePort {
        name: Some("http".to_string()),
        port: port.into(),
        protocol: Some("TCP".to_string()),
        ..ServicePort::default()
    }]
}

fn role_port(role_name: &str) -> Option<u16> {
    AirflowRole::from_str(role_name).unwrap().get_http_port()
}

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the administrator
#[allow(clippy::too_many_arguments)]
fn build_rolegroup_config_map(
    airflow: &AirflowCluster,
    resolved_product_image: &ResolvedProductImage,
    rolegroup: &RoleGroupRef<AirflowCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    authentication_config: &Vec<AirflowAuthenticationConfigResolved>,
    logging: &Logging<Container>,
    vector_aggregator_address: Option<&str>,
    container: &Container,
) -> Result<ConfigMap, Error> {
    let mut config = rolegroup_config
        .get(&PropertyNameKind::File(AIRFLOW_CONFIG_FILENAME.to_string()))
        .cloned()
        .unwrap_or_default();

    config::add_airflow_config(&mut config, authentication_config);

    let mut config_file = Vec::new();
    flask_app_config_writer::write::<AirflowConfigOptions, _, _>(
        &mut config_file,
        config.iter(),
        PYTHON_IMPORTS,
    )
    .with_context(|_| BuildRoleGroupConfigFileSnafu {
        rolegroup: rolegroup.clone(),
    })?;

    let mut cm_builder = ConfigMapBuilder::new();

    cm_builder
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
        );

    extend_config_map_with_log_config(
        rolegroup,
        vector_aggregator_address,
        logging,
        container,
        &Container::Vector,
        &mut cm_builder,
    )
    .context(InvalidLoggingConfigSnafu {
        cm_name: rolegroup.object_name(),
    })?;

    cm_builder
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
            // Internal communication does not need to be exposed
            type_: Some("ClusterIP".to_string()),
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
#[allow(clippy::too_many_arguments)]
fn build_server_rolegroup_statefulset(
    airflow: &AirflowCluster,
    resolved_product_image: &ResolvedProductImage,
    airflow_role: &AirflowRole,
    rolegroup_ref: &RoleGroupRef<AirflowCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    authentication_config: &Vec<AirflowAuthenticationConfigResolved>,
    sa_name: &str,
    config: &AirflowConfig,
    executor: &AirflowExecutor,
) -> Result<StatefulSet> {
    let binding = airflow.get_role(airflow_role);
    let role = binding.as_ref().context(NoAirflowRoleSnafu)?;

    let rolegroup = role.role_groups.get(&rolegroup_ref.role_group);
    let commands = airflow_role.get_commands();

    let mut pb = PodBuilder::new();
    pb.metadata_builder(|m| {
        m.with_recommended_labels(build_recommended_labels(
            airflow,
            AIRFLOW_CONTROLLER_NAME,
            &resolved_product_image.app_version_label,
            &rolegroup_ref.role,
            &rolegroup_ref.role_group,
        ))
    })
    .image_pull_secrets_from_product_image(resolved_product_image)
    .affinity(&config.affinity)
    .service_account_name(sa_name)
    .security_context(
        PodSecurityContextBuilder::new()
            .run_as_user(AIRFLOW_UID)
            .run_as_group(0)
            .fs_group(1000)
            .build(),
    );

    let mut airflow_container = ContainerBuilder::new(&Container::Airflow.to_string())
        .context(InvalidContainerNameSnafu)?;

    add_authentication_volumes_and_volume_mounts(
        authentication_config,
        &mut airflow_container,
        &mut pb,
    );

    let mut args = Vec::new();
    if let Some(ContainerLogConfig {
        choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
    }) = config.logging.containers.get(&Container::Airflow)
    {
        args.push(capture_shell_output(
            STACKABLE_LOG_DIR,
            &Container::Airflow.to_string(),
            log_config,
        ));
    }
    args.extend(commands);

    airflow_container
        .image_from_product_image(resolved_product_image)
        .resources(config.resources.clone().into())
        .command(vec!["/bin/bash".to_string()])
        .args(vec![String::from("-c"), args.join("; ")]);

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
    let env_mapped = build_mapped_envs(airflow, rolegroup_config, executor);

    airflow_container.add_env_vars(env_config);
    airflow_container.add_env_vars(env_mapped);
    airflow_container.add_env_vars(build_static_envs());

    let volume_mounts = airflow.volume_mounts();
    airflow_container.add_volume_mounts(volume_mounts);
    airflow_container.add_volume_mount(CONFIG_VOLUME_NAME, CONFIG_PATH);
    airflow_container.add_volume_mount(LOG_CONFIG_VOLUME_NAME, LOG_CONFIG_DIR);
    airflow_container.add_volume_mount(LOG_VOLUME_NAME, STACKABLE_LOG_DIR);

    if let AirflowExecutor::KubernetesExecutor { .. } = executor {
        airflow_container.add_volume_mount(TEMPLATE_VOLUME_NAME, TEMPLATE_LOCATION);
    }

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
        airflow_container.readiness_probe(probe.clone());
        airflow_container.liveness_probe(probe);
        airflow_container.add_container_port("http", resolved_port.into());
    }

    pb.add_container(airflow_container.build());

    let metrics_container = ContainerBuilder::new("metrics")
        .context(InvalidContainerNameSnafu)?
        .image_from_product_image(resolved_product_image)
        .command(vec!["/bin/bash".to_string(), "-c".to_string()])
        .args(vec!["/stackable/statsd_exporter".to_string()])
        .add_container_port(METRICS_PORT_NAME, METRICS_PORT)
        .resources(
            ResourceRequirementsBuilder::new()
                .with_cpu_request("100m")
                .with_cpu_limit("200m")
                .with_memory_request("64Mi")
                .with_memory_limit("64Mi")
                .build(),
        )
        .build();
    pb.add_container(metrics_container);

    pb.add_volumes(airflow.volumes());
    pb.add_volumes(controller_commons::create_volumes(
        &rolegroup_ref.object_name(),
        config.logging.containers.get(&Container::Airflow),
    ));

    if let AirflowExecutor::KubernetesExecutor { .. } = executor {
        pb.add_volume(
            VolumeBuilder::new(TEMPLATE_VOLUME_NAME)
                .with_config_map(TEMPLATE_CONFIGMAP_NAME)
                .build(),
        );
    }

    if let Some(gitsync) = airflow.git_sync() {
        let gitsync_container = ContainerBuilder::new(&format!("{}-{}", GIT_SYNC_NAME, 1))
            .context(InvalidContainerNameSnafu)?
            .add_env_vars(build_gitsync_envs(rolegroup_config))
            .image_from_product_image(resolved_product_image)
            .command(vec!["/bin/bash".to_string(), "-c".to_string()])
            .args(vec![gitsync.get_args().join(" ")])
            .add_volume_mount(GIT_CONTENT, GIT_ROOT)
            .resources(
                ResourceRequirementsBuilder::new()
                    .with_cpu_request("100m")
                    .with_cpu_limit("200m")
                    .with_memory_request("64Mi")
                    .with_memory_limit("64Mi")
                    .build(),
            )
            .build();

        pb.add_volume(
            VolumeBuilder::new(GIT_CONTENT)
                .empty_dir(EmptyDirVolumeSource::default())
                .build(),
        );
        pb.add_container(gitsync_container);
    }

    if config.logging.enable_vector_agent {
        pb.add_container(product_logging::framework::vector_container(
            resolved_product_image,
            CONFIG_VOLUME_NAME,
            LOG_VOLUME_NAME,
            config.logging.containers.get(&Container::Vector),
            ResourceRequirementsBuilder::new()
                .with_cpu_request("250m")
                .with_cpu_limit("500m")
                .with_memory_request("128Mi")
                .with_memory_limit("128Mi")
                .build(),
        ));
    }

    let mut pod_template = pb.build_template();
    pod_template.merge_from(role.config.pod_overrides.clone());
    if let Some(rolegroup) = rolegroup {
        pod_template.merge_from(rolegroup.config.pod_overrides.clone());
    }

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
            replicas: rolegroup.and_then(|rg| rg.replicas).map(i32::from),
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
            template: pod_template,
            ..StatefulSetSpec::default()
        }),
        status: None,
    })
}

#[allow(clippy::too_many_arguments)]
fn build_executor_template_config_map(
    airflow: &AirflowCluster,
    resolved_product_image: &ResolvedProductImage,
    authentication_config: &Vec<AirflowAuthenticationConfigResolved>,
    sa_name: &str,
    config: &ExecutorConfig,
    env_overrides: &HashMap<String, String>,
    rolegroup_ref: &RoleGroupRef<AirflowCluster>,
) -> Result<ConfigMap> {
    let mut pb = PodBuilder::new();
    pb.metadata_builder(|m| {
        m.with_recommended_labels(build_recommended_labels(
            airflow,
            AIRFLOW_CONTROLLER_NAME,
            &resolved_product_image.app_version_label,
            "executor",
            "executor-template",
        ))
    })
    .image_pull_secrets_from_product_image(resolved_product_image)
    .service_account_name(sa_name)
    .restart_policy("Never")
    .security_context(
        PodSecurityContextBuilder::new()
            .run_as_user(AIRFLOW_UID)
            .run_as_group(0)
            .fs_group(1000)
            .build(),
    );

    // N.B. this "base" name is an airflow requirement and should not be changed!
    // See https://airflow.apache.org/docs/apache-airflow/2.6.1/core-concepts/executor/kubernetes.html#base-image
    let mut airflow_container =
        ContainerBuilder::new(&Container::Base.to_string()).context(InvalidContainerNameSnafu)?;

    add_authentication_volumes_and_volume_mounts(
        authentication_config,
        &mut airflow_container,
        &mut pb,
    );

    airflow_container
        .image_from_product_image(resolved_product_image)
        .resources(config.resources.clone().into())
        .add_env_vars(build_template_envs(airflow, env_overrides))
        .add_volume_mounts(airflow.volume_mounts())
        .add_volume_mount(CONFIG_VOLUME_NAME, CONFIG_PATH)
        .add_volume_mount(LOG_CONFIG_VOLUME_NAME, LOG_CONFIG_DIR)
        .add_volume_mount(LOG_VOLUME_NAME, STACKABLE_LOG_DIR);

    if config.logging.enable_vector_agent {
        airflow_container.add_env_var(
            "_STACKABLE_POST_HOOK",
            [
                // Wait for Vector to gather the logs.
                "sleep 10",
                &shutdown_vector_command(STACKABLE_LOG_DIR),
            ]
            .join("; "),
        );
    }

    pb.add_container(airflow_container.build());
    pb.add_volumes(airflow.volumes());

    pb.add_volumes(controller_commons::create_volumes(
        &rolegroup_ref.object_name(),
        config.logging.containers.get(&Container::Airflow),
    ));

    if let Some(gitsync) = airflow.git_sync() {
        let mut env = vec![];
        if let Some(credentials_secret) = &gitsync.credentials_secret {
            env.push(env_var_from_secret(
                "GIT_SYNC_USERNAME",
                credentials_secret,
                "user",
            ));
            env.push(env_var_from_secret(
                "GIT_SYNC_PASSWORD",
                credentials_secret,
                "password",
            ));
        }
        let gitsync_container = ContainerBuilder::new(&format!("{}-{}", GIT_SYNC_NAME, 1))
            .context(InvalidContainerNameSnafu)?
            .add_env_vars(env)
            .image_from_product_image(resolved_product_image)
            .command(vec!["/bin/bash".to_string(), "-c".to_string()])
            .args(vec![gitsync.get_args().join(" ")])
            .add_volume_mount(GIT_CONTENT, GIT_ROOT)
            .resources(
                ResourceRequirementsBuilder::new()
                    .with_cpu_request("100m")
                    .with_cpu_limit("200m")
                    .with_memory_request("64Mi")
                    .with_memory_limit("64Mi")
                    .build(),
            )
            .build();

        pb.add_volume(
            VolumeBuilder::new(GIT_CONTENT)
                .empty_dir(EmptyDirVolumeSource::default())
                .build(),
        );
        pb.add_container(gitsync_container);
    }

    if config.logging.enable_vector_agent {
        pb.add_container(product_logging::framework::vector_container(
            resolved_product_image,
            CONFIG_VOLUME_NAME,
            LOG_VOLUME_NAME,
            config.logging.containers.get(&Container::Vector),
            ResourceRequirementsBuilder::new()
                .with_cpu_request("250m")
                .with_cpu_limit("500m")
                .with_memory_request("128Mi")
                .with_memory_limit("128Mi")
                .build(),
        ));
    }

    let pod_template = pb.build_template();
    let mut cm_builder = ConfigMapBuilder::new();

    cm_builder
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(airflow)
                .name(TEMPLATE_CONFIGMAP_NAME)
                .ownerreference_from_resource(airflow, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(build_recommended_labels(
                    airflow,
                    AIRFLOW_CONTROLLER_NAME,
                    &resolved_product_image.app_version_label,
                    "executor",
                    "executor-template",
                ))
                .with_label("restarter.stackable.tech/enabled", "true")
                .build(),
        )
        .add_data(
            TEMPLATE_NAME,
            serde_yaml::to_string(&pod_template).context(PodTemplateSerdeSnafu)?,
        );

    cm_builder.build().context(PodTemplateConfigMapSnafu)
}

/// This builds a collection of environment variables some require some minimal mapping,
/// such as executor type, contents of the secret etc.
fn build_mapped_envs(
    airflow: &AirflowCluster,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    executor: &AirflowExecutor,
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

    if let Some(git_sync) = &airflow.git_sync() {
        if let Some(dags_folder) = &git_sync.git_folder {
            env.push(EnvVar {
                name: "AIRFLOW__CORE__DAGS_FOLDER".into(),
                value: Some(format!("{GIT_SYNC_DIR}/{GIT_LINK}/{dags_folder}")),
                ..Default::default()
            })
        }
    }
    if let Some(true) = airflow.spec.cluster_config.load_examples {
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

    if let Some(true) = airflow.spec.cluster_config.expose_config {
        env.push(EnvVar {
            name: "AIRFLOW__WEBSERVER__EXPOSE_CONFIG".into(),
            value: Some("True".into()),
            ..Default::default()
        })
    }

    env.push(EnvVar {
        name: "AIRFLOW__CORE__EXECUTOR".into(),
        value: Some(executor.to_string()),
        ..Default::default()
    });

    if let AirflowExecutor::KubernetesExecutor { .. } = executor {
        env.push(EnvVar {
            name: "AIRFLOW__KUBERNETES_EXECUTOR__POD_TEMPLATE_FILE".into(),
            value: Some(format!("{TEMPLATE_LOCATION}/{TEMPLATE_NAME}")),
            ..Default::default()
        });
        // TODO: version < 2.5, delete when these versions are no longer supported
        env.push(EnvVar {
            name: "AIRFLOW__KUBERNETES__POD_TEMPLATE_FILE".into(),
            value: Some(format!("{TEMPLATE_LOCATION}/{TEMPLATE_NAME}")),
            ..Default::default()
        });
        env.push(EnvVar {
            name: "AIRFLOW__KUBERNETES_EXECUTOR__NAMESPACE".into(),
            value: airflow.namespace(),
            ..Default::default()
        });
        // TODO: version < 2.5, delete when these versions are no longer supported
        env.push(EnvVar {
            name: "AIRFLOW__KUBERNETES__NAMESPACE".into(),
            value: airflow.namespace(),
            ..Default::default()
        });
    }

    env
}

fn build_template_envs(
    airflow: &AirflowCluster,
    env_overrides: &HashMap<String, String>,
) -> Vec<EnvVar> {
    let secret_prop = Some(
        airflow
            .spec
            .cluster_config
            .credentials_secret
            .as_str()
            .clone(),
    );

    let mut env = secret_prop
        .map(|secret| {
            vec![env_var_from_secret(
                "AIRFLOW__CORE__SQL_ALCHEMY_CONN",
                secret,
                "connections.sqlalchemyDatabaseUri",
            )]
        })
        .unwrap_or_default();

    env.push(EnvVar {
        name: "AIRFLOW__CORE__EXECUTOR".into(),
        value: Some("LocalExecutor".to_string()),
        ..Default::default()
    });
    env.push(EnvVar {
        name: "AIRFLOW__KUBERNETES_EXECUTOR__NAMESPACE".into(),
        value: airflow.namespace(),
        ..Default::default()
    });
    // TODO: version < 2.5, delete when these versions are no longer supported
    env.push(EnvVar {
        name: "AIRFLOW__KUBERNETES__NAMESPACE".into(),
        value: airflow.namespace(),
        ..Default::default()
    });

    for (k, v) in env_overrides {
        env.push(EnvVar {
            name: k.to_string(),
            value: Some(v.to_string()),
            ..Default::default()
        });
    }

    if let Some(git_sync) = &airflow.git_sync() {
        if let Some(dags_folder) = &git_sync.git_folder {
            env.push(EnvVar {
                name: "AIRFLOW__CORE__DAGS_FOLDER".into(),
                value: Some(format!("{GIT_SYNC_DIR}/{GIT_LINK}/{dags_folder}")),
                ..Default::default()
            })
        }
    }

    env
}

fn build_gitsync_envs(
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Vec<EnvVar> {
    let mut env = vec![];
    if let Some(git_secret) = rolegroup_config
        .get(&PropertyNameKind::Env)
        .and_then(|vars| vars.get(AirflowConfig::GIT_CREDENTIALS_SECRET_PROPERTY))
    {
        env.push(env_var_from_secret("GIT_SYNC_USERNAME", git_secret, "user"));
        env.push(env_var_from_secret(
            "GIT_SYNC_PASSWORD",
            git_secret,
            "password",
        ));
    }

    env
}

fn build_static_envs() -> Vec<EnvVar> {
    [
        EnvVar {
            name: "PYTHONPATH".into(),
            value: Some(LOG_CONFIG_DIR.into()),
            ..Default::default()
        },
        EnvVar {
            name: "AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS".into(),
            value: Some("log_config.LOGGING_CONFIG".into()),
            ..Default::default()
        },
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
    authentication_config: &Vec<AirflowAuthenticationConfigResolved>,
    cb: &mut ContainerBuilder,
    pb: &mut PodBuilder,
) {
    // TODO: Currently there can be only one AuthenticationClass due to FlaskAppBuilder restrictions.
    //    Needs adaptation once FAB and airflow support multiple auth methods.
    // The checks for max one AuthenticationClass and the provider are done in crd/src/authentication.rs
    for config in authentication_config {
        if let Some(auth_class) = &config.authentication_class {
            match &auth_class.spec.provider {
                AuthenticationClassProvider::Ldap(ldap) => {
                    ldap.add_volumes_and_mounts(pb, vec![cb]);
                }
                AuthenticationClassProvider::Tls(_) | AuthenticationClassProvider::Static(_) => {}
            }
        }
    }
}

/// Return true if the controller should wait for the DB to be set up.
///
/// As a side-effect, the Airflow cluster status is updated as long as the controller waits
/// for the DB to come up.
///
/// Having the DB set up by a Job managed by a different controller has it's own
/// set of problems as described here: <https://github.com/stackabletech/superset-operator/issues/351>.
/// The Superset operator uses the same pattern as implemented here for setting up the DB.
///
/// When the ticket above is implemented, this function will most likely be removed completely.
async fn wait_for_db_and_update_status(
    client: &stackable_operator::client::Client,
    airflow: &AirflowCluster,
    resolved_product_image: &ResolvedProductImage,
    cluster_operation_condition_builder: &ClusterOperationsConditionBuilder<'_>,
) -> Result<bool> {
    // ensure admin user has been set up on the airflow database
    let airflow_db = AirflowDB::for_airflow(airflow, resolved_product_image)
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

    // Update the Airflow cluster status, only if the controller needs to wait.
    // This avoids updating the status twice per reconcile call. when the DB
    // has a ready condition.
    let db_cond_builder = DbConditionBuilder(airflow_db.status);
    if bool::from(&db_cond_builder) {
        let status = AirflowClusterStatus {
            conditions: compute_conditions(
                airflow,
                &[&db_cond_builder, cluster_operation_condition_builder],
            ),
        };

        client
            .apply_patch_status(OPERATOR_NAME, airflow, &status)
            .await
            .context(ApplyStatusSnafu)?;
    }
    Ok(bool::from(&db_cond_builder))
}

struct DbConditionBuilder(Option<AirflowDBStatus>);
impl ConditionBuilder for DbConditionBuilder {
    fn build_conditions(&self) -> ClusterConditionSet {
        let (status, message) = if let Some(ref status) = self.0 {
            match status.condition {
                AirflowDBStatusCondition::Pending | AirflowDBStatusCondition::Initializing => (
                    ClusterConditionStatus::False,
                    "Waiting for AirflowDB initialization to complete",
                ),
                AirflowDBStatusCondition::Failed => (
                    ClusterConditionStatus::False,
                    "Airflow database initialization failed.",
                ),
                AirflowDBStatusCondition::Ready => (
                    ClusterConditionStatus::True,
                    "Airflow database initialization ready.",
                ),
            }
        } else {
            (
                ClusterConditionStatus::Unknown,
                "Waiting for Airflow database initialization to start.",
            )
        };

        let cond = ClusterCondition {
            reason: None,
            message: Some(String::from(message)),
            status,
            type_: ClusterConditionType::Available,
            last_transition_time: None,
            last_update_time: None,
        };

        vec![cond].into()
    }
}

/// Evaluates to true if the DB is not ready yet (the controller needs to wait).
/// Otherwise false.
impl From<&DbConditionBuilder> for bool {
    fn from(cond_builder: &DbConditionBuilder) -> bool {
        if let Some(ref status) = cond_builder.0 {
            match status.condition {
                AirflowDBStatusCondition::Pending | AirflowDBStatusCondition::Initializing => true,
                AirflowDBStatusCondition::Failed => true,
                AirflowDBStatusCondition::Ready => false,
            }
        } else {
            true
        }
    }
}
