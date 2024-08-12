//! Ensures that `Pod`s are configured and running for each [`AirflowCluster`]
use product_config::{
    flask_app_config_writer::{self, FlaskAppConfigWriterError},
    types::PropertyNameKind,
    ProductConfigManager,
};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_airflow_crd::git_sync::GitSync;
use stackable_airflow_crd::{
    authentication::AirflowAuthenticationConfigResolved, build_recommended_labels, AirflowCluster,
    AirflowClusterStatus, AirflowConfig, AirflowConfigFragment, AirflowConfigOptions,
    AirflowExecutor, AirflowRole, Container, ExecutorConfig, ExecutorConfigFragment,
    AIRFLOW_CONFIG_FILENAME, AIRFLOW_UID, APP_NAME, CONFIG_PATH, GIT_CONTENT, GIT_ROOT,
    GIT_SYNC_NAME, LOG_CONFIG_DIR, OPERATOR_NAME, STACKABLE_LOG_DIR, TEMPLATE_CONFIGMAP_NAME,
    TEMPLATE_LOCATION, TEMPLATE_NAME, TEMPLATE_VOLUME_NAME,
};
use stackable_operator::k8s_openapi::api::core::v1::{EnvVar, PodTemplateSpec, VolumeMount};
use stackable_operator::kube::api::ObjectMeta;
use stackable_operator::{
    builder::{
        configmap::ConfigMapBuilder,
        meta::ObjectMetaBuilder,
        pod::{
            container::ContainerBuilder, resources::ResourceRequirementsBuilder,
            security::PodSecurityContextBuilder, volume::VolumeBuilder, PodBuilder,
        },
    },
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::{
        authentication::{ldap, AuthenticationClass, AuthenticationClassProvider},
        product_image_selection::ResolvedProductImage,
        rbac::build_rbac_resources,
    },
    config::fragment::ValidationError,
    k8s_openapi,
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, EmptyDirVolumeSource, Probe, Service, ServicePort, ServiceSpec,
                TCPSocketAction,
            },
        },
        apimachinery::pkg::{apis::meta::v1::LabelSelector, util::intstr::IntOrString},
        DeepMerge,
    },
    kube::{
        runtime::{controller::Action, reflector::ObjectRef},
        Resource, ResourceExt,
    },
    kvp::{Label, LabelError, Labels},
    logging::controller::ReconcilerError,
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    product_logging::{
        self,
        spec::{ContainerLogConfig, Logging},
    },
    role_utils::{CommonConfiguration, GenericRoleConfig, RoleGroupRef},
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
    time::Duration,
    utils::COMMON_BASH_TRAP_FUNCTIONS,
};
use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::Arc,
};
use strum::{EnumDiscriminants, IntoEnumIterator, IntoStaticStr};

use crate::env_vars::{
    build_airflow_template_envs, build_gitsync_statefulset_envs, build_gitsync_template,
};
use crate::{
    config::{self, PYTHON_IMPORTS},
    controller_commons::{self, CONFIG_VOLUME_NAME, LOG_CONFIG_VOLUME_NAME, LOG_VOLUME_NAME},
    env_vars,
    operations::{
        graceful_shutdown::{
            add_airflow_graceful_shutdown_config, add_executor_graceful_shutdown_config,
        },
        pdb::add_pdbs,
    },
    product_logging::{extend_config_map_with_log_config, resolve_vector_aggregator_address},
};

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
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to apply Service for {rolegroup}"))]
    ApplyRoleGroupService {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupRef<AirflowCluster>,
    },

    #[snafu(display("failed to apply ConfigMap for {rolegroup}"))]
    ApplyRoleGroupConfig {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupRef<AirflowCluster>,
    },

    #[snafu(display("failed to apply StatefulSet for {rolegroup}"))]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupRef<AirflowCluster>,
    },

    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("Failed to transform configs"))]
    ProductConfigTransform {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("failed to patch service account"))]
    ApplyServiceAccount {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to patch role binding: {source}"))]
    ApplyRoleBinding {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to build RBAC objects"))]
    BuildRBACObjects {
        source: stackable_operator::commons::rbac::Error,
    },

    #[snafu(display("failed to retrieve AuthenticationClass {authentication_class}"))]
    AuthenticationClassRetrieval {
        source: stackable_operator::cluster_resources::Error,
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
        source: stackable_operator::builder::configmap::Error,
        rolegroup: RoleGroupRef<AirflowCluster>,
    },

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
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("invalid container name"))]
    InvalidContainerName {
        source: stackable_operator::builder::pod::container::Error,
    },

    #[snafu(display("failed to create cluster resources"))]
    CreateClusterResources {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::cluster_resources::Error,
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
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to apply authentication configuration"))]
    InvalidAuthenticationConfig {
        source: stackable_airflow_crd::authentication::Error,
    },

    #[snafu(display("pod template serialization"))]
    PodTemplateSerde { source: serde_yaml::Error },

    #[snafu(display("failed to build the pod template config map"))]
    PodTemplateConfigMap {
        source: stackable_operator::builder::configmap::Error,
    },

    #[snafu(display("failed to apply executor template ConfigMap"))]
    ApplyExecutorTemplateConfig {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("fragment validation failure"))]
    FragmentValidationFailure { source: ValidationError },

    #[snafu(display("failed to create PodDisruptionBudget"))]
    FailedToCreatePdb {
        source: crate::operations::pdb::Error,
    },

    #[snafu(display("failed to configure graceful shutdown"))]
    GracefulShutdown {
        source: crate::operations::graceful_shutdown::Error,
    },

    #[snafu(display("failed to build label"))]
    BuildLabel { source: LabelError },

    #[snafu(display("failed to build object meta data"))]
    ObjectMeta {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display(
        "failed to build volume or volume mount spec for the LDAP backend TLS config"
    ))]
    VolumeAndMounts { source: ldap::Error },

    #[snafu(display("failed to construct config"))]
    ConstructConfig { source: config::Error },
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
        .resolve(DOCKER_IMAGE_BASE_NAME, crate::built_info::PKG_VERSION);

    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&airflow.spec.cluster_operation);

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

    let role_config = transform_all_roles_to_config::<AirflowConfigFragment, _>(&airflow, roles);
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

    let required_labels = cluster_resources
        .get_required_labels()
        .context(BuildLabelSnafu)?;

    let (rbac_sa, rbac_rolebinding) =
        build_rbac_resources(airflow.as_ref(), APP_NAME, required_labels)
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
            common_configuration,
            &resolved_product_image,
            &authentication_config,
            &vector_aggregator_address,
            &mut cluster_resources,
            client,
            &rbac_sa,
        )
        .await?;
    }

    for (role_name, role_config) in validated_role_config.iter() {
        let airflow_role =
            AirflowRole::from_str(role_name).context(UnidentifiedAirflowRoleSnafu {
                role: role_name.to_string(),
            })?;

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

            let merged_airflow_config = airflow
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
                &merged_airflow_config,
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
                &merged_airflow_config.logging,
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

        let role_config = airflow.role_config(&airflow_role);
        if let Some(GenericRoleConfig {
            pod_disruption_budget: pdb,
        }) = role_config
        {
            add_pdbs(pdb, &airflow, &airflow_role, client, &mut cluster_resources)
                .await
                .context(FailedToCreatePdbSnafu)?;
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
    common_config: &CommonConfiguration<ExecutorConfigFragment>,
    resolved_product_image: &ResolvedProductImage,
    authentication_config: &Vec<AirflowAuthenticationConfigResolved>,
    vector_aggregator_address: &Option<String>,
    cluster_resources: &mut ClusterResources,
    client: &stackable_operator::client::Client,
    rbac_sa: &stackable_operator::k8s_openapi::api::core::v1::ServiceAccount,
) -> Result<(), Error> {
    let merged_executor_config = airflow
        .merged_executor_config(&common_config.config)
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
        &merged_executor_config.logging,
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
        &merged_executor_config,
        &common_config.env_overrides,
        &common_config.pod_overrides,
        &rolegroup,
    )?;
    cluster_resources
        .add(client, worker_pod_template_config_map)
        .await
        .with_context(|_| ApplyExecutorTemplateConfigSnafu {})?;
    Ok(())
}

/// The server-role service is the primary endpoint that should be used by clients that do not perform internal load balancing,
/// including targets outside the cluster.
fn build_role_service(
    airflow: &AirflowCluster,
    resolved_product_image: &ResolvedProductImage,
    role_name: &str,
    port: u16,
) -> Result<Service> {
    let role_svc_name = format!("{}-{}", airflow.name_any(), role_name);
    let ports = role_ports(port);

    let metadata = ObjectMetaBuilder::new()
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
        .context(ObjectMetaSnafu)?
        .build();

    let service_selector_labels =
        Labels::role_selector(airflow, APP_NAME, role_name).context(BuildLabelSnafu)?;

    let service_spec = ServiceSpec {
        type_: Some(
            airflow
                .spec
                .cluster_config
                .listener_class
                .k8s_service_type(),
        ),
        ports: Some(ports),
        selector: Some(service_selector_labels.into()),
        ..ServiceSpec::default()
    };

    Ok(Service {
        metadata,
        spec: Some(service_spec),
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

    config::add_airflow_config(&mut config, authentication_config).context(ConstructConfigSnafu)?;

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
                .context(ObjectMetaSnafu)?
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

    let prometheus_label =
        Label::try_from(("prometheus.io/scrape", "true")).context(BuildLabelSnafu)?;

    let metadata = build_rolegroup_metadata(
        airflow,
        &resolved_product_image,
        &rolegroup,
        prometheus_label,
    )?;

    let service_selector_labels =
        Labels::role_group_selector(airflow, APP_NAME, &rolegroup.role, &rolegroup.role_group)
            .context(BuildLabelSnafu)?;

    let service_spec = ServiceSpec {
        // Internal communication does not need to be exposed
        type_: Some("ClusterIP".to_string()),
        cluster_ip: Some("None".to_string()),
        ports: Some(ports),
        selector: Some(service_selector_labels.into()),
        publish_not_ready_addresses: Some(true),
        ..ServiceSpec::default()
    };

    Ok(Service {
        metadata,
        spec: Some(service_spec),
        status: None,
    })
}

fn build_rolegroup_metadata(
    airflow: &AirflowCluster,
    resolved_product_image: &&ResolvedProductImage,
    rolegroup: &&RoleGroupRef<AirflowCluster>,
    prometheus_label: Label,
) -> Result<ObjectMeta, Error> {
    let metadata = ObjectMetaBuilder::new()
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
        .context(ObjectMetaSnafu)?
        .with_label(prometheus_label)
        .build();
    Ok(metadata)
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
    merged_airflow_config: &AirflowConfig,
    executor: &AirflowExecutor,
) -> Result<StatefulSet> {
    let binding = airflow.get_role(airflow_role);
    let role = binding.as_ref().context(NoAirflowRoleSnafu)?;

    let rolegroup = role.role_groups.get(&rolegroup_ref.role_group);

    let mut pb = PodBuilder::new();

    let pb_metadata = ObjectMetaBuilder::new()
        .with_recommended_labels(build_recommended_labels(
            airflow,
            AIRFLOW_CONTROLLER_NAME,
            &resolved_product_image.app_version_label,
            &rolegroup_ref.role,
            &rolegroup_ref.role_group,
        ))
        .context(ObjectMetaSnafu)?
        .build();

    pb.metadata(pb_metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .affinity(&merged_airflow_config.affinity)
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
    )?;

    add_airflow_graceful_shutdown_config(merged_airflow_config, &mut pb)
        .context(GracefulShutdownSnafu)?;

    let mut airflow_container_args = Vec::new();
    airflow_container_args.extend(airflow_role.get_commands());

    airflow_container
        .image_from_product_image(resolved_product_image)
        .resources(merged_airflow_config.resources.clone().into())
        .command(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
        ])
        .args(vec![airflow_container_args.join("\n")]);

    airflow_container.add_env_vars(env_vars::build_airflow_statefulset_envs(
        airflow,
        airflow_role,
        rolegroup_config,
        executor,
    ));

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
            initial_delay_seconds: Some(60),
            period_seconds: Some(10),
            failure_threshold: Some(6),
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
        .command(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
        ])
        .args(vec![[
            COMMON_BASH_TRAP_FUNCTIONS.to_string(),
            "prepare_signal_handlers".to_string(),
            "/stackable/statsd_exporter &".to_string(),
            "wait_for_termination $!".to_string(),
        ]
        .join("\n")])
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

    pb.add_volumes(airflow.volumes().clone());
    pb.add_volumes(controller_commons::create_volumes(
        &rolegroup_ref.object_name(),
        merged_airflow_config
            .logging
            .containers
            .get(&Container::Airflow),
    ));

    if let AirflowExecutor::KubernetesExecutor { .. } = executor {
        pb.add_volume(
            VolumeBuilder::new(TEMPLATE_VOLUME_NAME)
                .with_config_map(TEMPLATE_CONFIGMAP_NAME)
                .build(),
        );
    }

    if let Some(gitsync) = airflow.git_sync() {
        let gitsync_container = build_gitsync_container(
            resolved_product_image,
            &gitsync,
            false,
            &format!("{}-{}", GIT_SYNC_NAME, 1),
            build_gitsync_statefulset_envs(rolegroup_config),
            airflow.volume_mounts(),
        )?;

        pb.add_volume(
            VolumeBuilder::new(GIT_CONTENT)
                .empty_dir(EmptyDirVolumeSource::default())
                .build(),
        );

        pb.add_container(gitsync_container);

        if let AirflowExecutor::CeleryExecutor { .. } = executor {
            let gitsync_init_container = build_gitsync_container(
                resolved_product_image,
                &gitsync,
                true,
                &format!("{}-{}", GIT_SYNC_NAME, 0),
                build_gitsync_statefulset_envs(rolegroup_config),
                airflow.volume_mounts(),
            )?;
            // If the DAG is modularized we may encounter a timing issue whereby the celery worker has started
            // *before* all modules referenced by the DAG have been fetched by gitsync and registered. This
            // will result in ModuleNotFoundError errors. This can be avoided by running a one-off git-sync
            // process in an init-container so that all DAG dependencies are fully loaded. The sidecar
            // git-sync is then used for regular updates.
            pb.add_init_container(gitsync_init_container);
        }
    }

    if merged_airflow_config.logging.enable_vector_agent {
        pb.add_container(build_logging_container(
            resolved_product_image,
            merged_airflow_config
                .logging
                .containers
                .get(&Container::Vector),
        ));
    }

    let mut pod_template = pb.build_template();
    pod_template.merge_from(role.config.pod_overrides.clone());
    if let Some(rolegroup) = rolegroup {
        pod_template.merge_from(rolegroup.config.pod_overrides.clone());
    }

    let restarter_label =
        Label::try_from(("restarter.stackable.tech/enabled", "true")).context(BuildLabelSnafu)?;

    let metadata = build_rolegroup_metadata(
        airflow,
        &resolved_product_image,
        &rolegroup_ref,
        restarter_label,
    )?;

    let statefulset_match_labels = Labels::role_group_selector(
        airflow,
        APP_NAME,
        &rolegroup_ref.role,
        &rolegroup_ref.role_group,
    )
    .context(BuildLabelSnafu)?;

    let statefulset_spec = StatefulSetSpec {
        pod_management_policy: Some(
            match airflow_role {
                AirflowRole::Scheduler => {
                    "OrderedReady" // Scheduler pods should start after another, since part of their startup phase is initializing the database, see crd/src/lib.rs
                }
                AirflowRole::Webserver | AirflowRole::Worker => "Parallel",
            }
            .to_string(),
        ),
        replicas: rolegroup.and_then(|rg| rg.replicas).map(i32::from),
        selector: LabelSelector {
            match_labels: Some(statefulset_match_labels.into()),
            ..LabelSelector::default()
        },
        service_name: rolegroup_ref.object_name(),
        template: pod_template,
        ..StatefulSetSpec::default()
    };

    Ok(StatefulSet {
        metadata,
        spec: Some(statefulset_spec),
        status: None,
    })
}

fn build_logging_container(
    resolved_product_image: &ResolvedProductImage,
    log_config: Option<&ContainerLogConfig>,
) -> k8s_openapi::api::core::v1::Container {
    product_logging::framework::vector_container(
        resolved_product_image,
        CONFIG_VOLUME_NAME,
        LOG_VOLUME_NAME,
        log_config,
        ResourceRequirementsBuilder::new()
            .with_cpu_request("250m")
            .with_cpu_limit("500m")
            .with_memory_request("128Mi")
            .with_memory_limit("128Mi")
            .build(),
    )
}

#[allow(clippy::too_many_arguments)]
fn build_executor_template_config_map(
    airflow: &AirflowCluster,
    resolved_product_image: &ResolvedProductImage,
    authentication_config: &Vec<AirflowAuthenticationConfigResolved>,
    sa_name: &str,
    merged_executor_config: &ExecutorConfig,
    env_overrides: &HashMap<String, String>,
    pod_overrides: &PodTemplateSpec,
    rolegroup_ref: &RoleGroupRef<AirflowCluster>,
) -> Result<ConfigMap> {
    let mut pb = PodBuilder::new();
    let pb_metadata = ObjectMetaBuilder::new()
        .with_recommended_labels(build_recommended_labels(
            airflow,
            AIRFLOW_CONTROLLER_NAME,
            &resolved_product_image.app_version_label,
            "executor",
            "executor-template",
        ))
        .context(ObjectMetaSnafu)?
        .build();

    pb.metadata(pb_metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .affinity(&merged_executor_config.affinity)
        .service_account_name(sa_name)
        .restart_policy("Never")
        .security_context(
            PodSecurityContextBuilder::new()
                .run_as_user(AIRFLOW_UID)
                .run_as_group(0)
                .fs_group(1000)
                .build(),
        );

    add_executor_graceful_shutdown_config(merged_executor_config, &mut pb)
        .context(GracefulShutdownSnafu)?;

    // N.B. this "base" name is an airflow requirement and should not be changed!
    // See https://airflow.apache.org/docs/apache-airflow/2.6.1/core-concepts/executor/kubernetes.html#base-image
    let mut airflow_container =
        ContainerBuilder::new(&Container::Base.to_string()).context(InvalidContainerNameSnafu)?;

    add_authentication_volumes_and_volume_mounts(
        authentication_config,
        &mut airflow_container,
        &mut pb,
    )?;

    airflow_container
        .image_from_product_image(resolved_product_image)
        .resources(merged_executor_config.resources.clone().into())
        .add_env_vars(build_airflow_template_envs(
            airflow,
            env_overrides,
            merged_executor_config,
        ))
        .add_volume_mounts(airflow.volume_mounts())
        .add_volume_mount(CONFIG_VOLUME_NAME, CONFIG_PATH)
        .add_volume_mount(LOG_CONFIG_VOLUME_NAME, LOG_CONFIG_DIR)
        .add_volume_mount(LOG_VOLUME_NAME, STACKABLE_LOG_DIR);

    pb.add_container(airflow_container.build());
    pb.add_volumes(airflow.volumes().clone());
    pb.add_volumes(controller_commons::create_volumes(
        &rolegroup_ref.object_name(),
        merged_executor_config
            .logging
            .containers
            .get(&Container::Airflow),
    ));

    if let Some(gitsync) = airflow.git_sync() {
        let gitsync_container = build_gitsync_container(
            resolved_product_image,
            &gitsync,
            true,
            &format!("{}-{}", GIT_SYNC_NAME, 0),
            build_gitsync_template(env_overrides),
            airflow.volume_mounts(),
        )?;
        pb.add_volume(
            VolumeBuilder::new(GIT_CONTENT)
                .empty_dir(EmptyDirVolumeSource::default())
                .build(),
        );
        pb.add_init_container(gitsync_container);
    }

    if merged_executor_config.logging.enable_vector_agent {
        pb.add_container(build_logging_container(
            resolved_product_image,
            merged_executor_config
                .logging
                .containers
                .get(&Container::Vector),
        ));
    }

    let mut pod_template = pb.build_template();
    pod_template.merge_from(pod_overrides.clone());

    let mut cm_builder = ConfigMapBuilder::new();

    let restarter_label =
        Label::try_from(("restarter.stackable.tech/enabled", "true")).context(BuildLabelSnafu)?;

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
                .context(ObjectMetaSnafu)?
                .with_label(restarter_label)
                .build(),
        )
        .add_data(
            TEMPLATE_NAME,
            serde_yaml::to_string(&pod_template).context(PodTemplateSerdeSnafu)?,
        );

    cm_builder.build().context(PodTemplateConfigMapSnafu)
}

fn build_gitsync_container(
    resolved_product_image: &ResolvedProductImage,
    gitsync: &&GitSync,
    one_time: bool,
    name: &str,
    env_vars: Vec<EnvVar>,
    volume_mounts: Vec<VolumeMount>,
) -> Result<k8s_openapi::api::core::v1::Container, Error> {
    let gitsync_container = ContainerBuilder::new(name)
        .context(InvalidContainerNameSnafu)?
        .add_env_vars(env_vars)
        .image_from_product_image(resolved_product_image)
        .command(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
        ])
        .args(vec![gitsync.get_args(one_time).join("\n")])
        .add_volume_mount(GIT_CONTENT, GIT_ROOT)
        .add_volume_mounts(volume_mounts)
        .resources(
            ResourceRequirementsBuilder::new()
                .with_cpu_request("100m")
                .with_cpu_limit("200m")
                .with_memory_request("64Mi")
                .with_memory_limit("64Mi")
                .build(),
        )
        .build();
    Ok(gitsync_container)
}

pub fn error_policy(_obj: Arc<AirflowCluster>, _error: &Error, _ctx: Arc<Ctx>) -> Action {
    Action::requeue(*Duration::from_secs(5))
}

fn add_authentication_volumes_and_volume_mounts(
    authentication_config: &Vec<AirflowAuthenticationConfigResolved>,
    cb: &mut ContainerBuilder,
    pb: &mut PodBuilder,
) -> Result<()> {
    // TODO: Currently there can be only one AuthenticationClass due to FlaskAppBuilder restrictions.
    //    Needs adaptation once FAB and airflow support multiple auth methods.
    // The checks for max one AuthenticationClass and the provider are done in crd/src/authentication.rs
    for config in authentication_config {
        if let Some(auth_class) = &config.authentication_class {
            match &auth_class.spec.provider {
                AuthenticationClassProvider::Ldap(ldap) => {
                    ldap.add_volumes_and_mounts(pb, vec![cb])
                        .context(VolumeAndMountsSnafu)?;
                }
                AuthenticationClassProvider::Tls(_)
                | AuthenticationClassProvider::Oidc(_)
                | AuthenticationClassProvider::Static(_) => {}
            }
        }
    }
    Ok(())
}
