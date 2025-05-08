//! Ensures that `Pod`s are configured and running for each [`v1alpha1::AirflowCluster`]
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    io::Write,
    str::FromStr,
    sync::Arc,
};

use const_format::concatcp;
use product_config::{
    ProductConfigManager,
    flask_app_config_writer::{self, FlaskAppConfigWriterError},
    types::PropertyNameKind,
};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        self,
        configmap::ConfigMapBuilder,
        meta::ObjectMetaBuilder,
        pod::{
            PodBuilder, container::ContainerBuilder, resources::ResourceRequirementsBuilder,
            security::PodSecurityContextBuilder, volume::VolumeBuilder,
        },
    },
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::{product_image_selection::ResolvedProductImage, rbac::build_rbac_resources},
    config::fragment::ValidationError,
    crd::authentication::{core as auth_core, ldap},
    k8s_openapi::{
        self, DeepMerge,
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, EmptyDirVolumeSource, EnvVar, PodTemplateSpec, Probe, Service,
                ServiceAccount, ServicePort, ServiceSpec, TCPSocketAction, VolumeMount,
            },
        },
        apimachinery::pkg::{apis::meta::v1::LabelSelector, util::intstr::IntOrString},
    },
    kube::{
        Resource, ResourceExt,
        api::ObjectMeta,
        core::{DeserializeGuard, error_boundary},
        runtime::{controller::Action, reflector::ObjectRef},
    },
    kvp::{Label, LabelError, Labels},
    logging::controller::ReconcilerError,
    product_config_utils::{
        CONFIG_OVERRIDE_FILE_FOOTER_KEY, CONFIG_OVERRIDE_FILE_HEADER_KEY,
        transform_all_roles_to_config, validate_all_roles_and_groups_config,
    },
    product_logging::{
        self,
        framework::LoggingError,
        spec::{ContainerLogConfig, Logging},
    },
    role_utils::{
        CommonConfiguration, GenericProductSpecificCommonConfig, GenericRoleConfig, RoleGroupRef,
    },
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
    time::Duration,
    utils::COMMON_BASH_TRAP_FUNCTIONS,
};
use strum::{EnumDiscriminants, IntoEnumIterator, IntoStaticStr};

use crate::{
    config::{self, PYTHON_IMPORTS},
    controller_commons::{self, CONFIG_VOLUME_NAME, LOG_CONFIG_VOLUME_NAME, LOG_VOLUME_NAME},
    crd::{
        self, AIRFLOW_CONFIG_FILENAME, AIRFLOW_UID, APP_NAME, AirflowClusterStatus, AirflowConfig,
        AirflowConfigOptions, AirflowExecutor, AirflowRole, CONFIG_PATH, Container, ExecutorConfig,
        ExecutorConfigFragment, LOG_CONFIG_DIR, OPERATOR_NAME, STACKABLE_LOG_DIR,
        TEMPLATE_CONFIGMAP_NAME, TEMPLATE_LOCATION, TEMPLATE_NAME, TEMPLATE_VOLUME_NAME,
        authentication::{
            AirflowAuthenticationClassResolved, AirflowClientAuthenticationDetailsResolved,
        },
        authorization::AirflowAuthorizationResolved,
        build_recommended_labels,
        git_sync::{GIT_SYNC_CONTENT, GIT_SYNC_NAME, GIT_SYNC_ROOT, GitSync},
        v1alpha1,
    },
    env_vars::{
        self, build_airflow_template_envs, build_gitsync_statefulset_envs, build_gitsync_template,
    },
    operations::{
        graceful_shutdown::{
            add_airflow_graceful_shutdown_config, add_executor_graceful_shutdown_config,
        },
        pdb::add_pdbs,
    },
    product_logging::extend_config_map_with_log_config,
};

pub const AIRFLOW_CONTROLLER_NAME: &str = "airflowcluster";
pub const DOCKER_IMAGE_BASE_NAME: &str = "airflow";
pub const AIRFLOW_FULL_CONTROLLER_NAME: &str =
    concatcp!(AIRFLOW_CONTROLLER_NAME, '.', OPERATOR_NAME);

const METRICS_PORT_NAME: &str = "metrics";
const METRICS_PORT: i32 = 9102;

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
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
        rolegroup: RoleGroupRef<v1alpha1::AirflowCluster>,
    },

    #[snafu(display("failed to apply ConfigMap for {rolegroup}"))]
    ApplyRoleGroupConfig {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupRef<v1alpha1::AirflowCluster>,
    },

    #[snafu(display("failed to apply StatefulSet for {rolegroup}"))]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupRef<v1alpha1::AirflowCluster>,
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
        authentication_class: ObjectRef<auth_core::v1alpha1::AuthenticationClass>,
    },

    #[snafu(display(
        "Airflow doesn't support the AuthenticationClass provider
    {authentication_class_provider} from AuthenticationClass {authentication_class}"
    ))]
    AuthenticationClassProviderNotSupported {
        authentication_class_provider: String,
        authentication_class: ObjectRef<auth_core::v1alpha1::AuthenticationClass>,
    },

    #[snafu(display("failed to build config file for {rolegroup}"))]
    BuildRoleGroupConfigFile {
        source: FlaskAppConfigWriterError,
        rolegroup: RoleGroupRef<v1alpha1::AirflowCluster>,
    },

    #[snafu(display("failed to build ConfigMap for {rolegroup}"))]
    BuildRoleGroupConfig {
        source: stackable_operator::builder::configmap::Error,
        rolegroup: RoleGroupRef<v1alpha1::AirflowCluster>,
    },

    #[snafu(display("failed to resolve and merge config for role and role group"))]
    FailedToResolveConfig { source: crd::Error },

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

    #[snafu(display("vector agent is enabled but vector aggregator ConfigMap is missing"))]
    VectorAggregatorConfigMapMissing,

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
    InvalidAuthenticationConfig { source: crd::authentication::Error },

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
    VolumeAndMounts { source: ldap::v1alpha1::Error },

    #[snafu(display("failed to construct config"))]
    ConstructConfig { source: config::Error },

    #[snafu(display(
        "failed to write to String (Vec<u8> to be precise) containing Airflow config"
    ))]
    WriteToConfigFileString { source: std::io::Error },

    #[snafu(display("failed to configure logging"))]
    ConfigureLogging { source: LoggingError },

    #[snafu(display("failed to add needed volume"))]
    AddVolume { source: builder::pod::Error },

    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: builder::pod::container::Error,
    },

    #[snafu(display("failed to add LDAP Volumes and VolumeMounts"))]
    AddLdapVolumesAndVolumeMounts { source: ldap::v1alpha1::Error },

    #[snafu(display("failed to add TLS Volumes and VolumeMounts"))]
    AddTlsVolumesAndVolumeMounts {
        source: stackable_operator::commons::tls_verification::TlsClientDetailsError,
    },

    #[snafu(display("AirflowCluster object is invalid"))]
    InvalidAirflowCluster {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("failed to build Statefulset environmental variables"))]
    BuildStatefulsetEnvVars { source: env_vars::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

pub async fn reconcile_airflow(
    airflow: Arc<DeserializeGuard<v1alpha1::AirflowCluster>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    tracing::info!("Starting reconcile");

    let airflow = airflow
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidAirflowClusterSnafu)?;

    let client = &ctx.client;
    let resolved_product_image: ResolvedProductImage = airflow
        .spec
        .image
        .resolve(DOCKER_IMAGE_BASE_NAME, crate::built_info::PKG_VERSION);

    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&airflow.spec.cluster_operation);

    let authentication_config = AirflowClientAuthenticationDetailsResolved::from(
        &airflow.spec.cluster_config.authentication,
        client,
    )
    .await
    .context(InvalidAuthenticationConfigSnafu)?;

    let authorization_config = AirflowAuthorizationResolved::from_authorization_config(
        client,
        airflow,
        &airflow.spec.cluster_config.authorization,
    )
    .await
    .unwrap();

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

    let role_config = transform_all_roles_to_config(airflow, roles);
    let validated_role_config = validate_all_roles_and_groups_config(
        &resolved_product_image.product_version,
        &role_config.context(ProductConfigTransformSnafu)?,
        &ctx.product_config,
        false,
        false,
    )
    .context(InvalidProductConfigSnafu)?;

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
        build_rbac_resources(airflow, APP_NAME, required_labels).context(BuildRBACObjectsSnafu)?;

    let rbac_sa = cluster_resources
        .add(client, rbac_sa.clone())
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
            airflow,
            common_configuration,
            &resolved_product_image,
            &authentication_config,
            &authorization_config,
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
                build_role_service(airflow, &resolved_product_image, role_name, resolved_port)?;
            cluster_resources
                .add(client, role_service)
                .await
                .context(ApplyRoleServiceSnafu)?;
        }

        for (rolegroup_name, rolegroup_config) in role_config.iter() {
            let rolegroup = RoleGroupRef {
                cluster: ObjectRef::from_obj(airflow),
                role: role_name.into(),
                role_group: rolegroup_name.into(),
            };

            let merged_airflow_config = airflow
                .merged_config(&airflow_role, &rolegroup)
                .context(FailedToResolveConfigSnafu)?;

            let rg_service = build_rolegroup_service(airflow, &resolved_product_image, &rolegroup)?;
            cluster_resources.add(client, rg_service).await.context(
                ApplyRoleGroupServiceSnafu {
                    rolegroup: rolegroup.clone(),
                },
            )?;

            let rg_statefulset = build_server_rolegroup_statefulset(
                airflow,
                &resolved_product_image,
                &airflow_role,
                &rolegroup,
                rolegroup_config,
                &authentication_config,
                &authorization_config,
                &rbac_sa,
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
                airflow,
                &resolved_product_image,
                &rolegroup,
                rolegroup_config,
                &authentication_config,
                &authorization_config,
                &merged_airflow_config.logging,
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
            add_pdbs(pdb, airflow, &airflow_role, client, &mut cluster_resources)
                .await
                .context(FailedToCreatePdbSnafu)?;
        }
    }

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphanedResourcesSnafu)?;

    let status = AirflowClusterStatus {
        conditions: compute_conditions(airflow, &[
            &ss_cond_builder,
            &cluster_operation_cond_builder,
        ]),
    };

    client
        .apply_patch_status(OPERATOR_NAME, airflow, &status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(Action::await_change())
}

#[allow(clippy::too_many_arguments)]
async fn build_executor_template(
    airflow: &v1alpha1::AirflowCluster,
    common_config: &CommonConfiguration<ExecutorConfigFragment, GenericProductSpecificCommonConfig>,
    resolved_product_image: &ResolvedProductImage,
    authentication_config: &AirflowClientAuthenticationDetailsResolved,
    authorization_config: &AirflowAuthorizationResolved,
    cluster_resources: &mut ClusterResources,
    client: &stackable_operator::client::Client,
    rbac_sa: &stackable_operator::k8s_openapi::api::core::v1::ServiceAccount,
) -> Result<(), Error> {
    let merged_executor_config = airflow
        .merged_executor_config(&common_config.config)
        .context(FailedToResolveConfigSnafu)?;
    let rolegroup = RoleGroupRef {
        cluster: ObjectRef::from_obj(airflow),
        role: "executor".into(),
        role_group: "kubernetes".into(),
    };

    let rg_configmap = build_rolegroup_config_map(
        airflow,
        resolved_product_image,
        &rolegroup,
        &HashMap::new(),
        authentication_config,
        authorization_config,
        &merged_executor_config.logging,
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
    airflow: &v1alpha1::AirflowCluster,
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
    airflow: &v1alpha1::AirflowCluster,
    resolved_product_image: &ResolvedProductImage,
    rolegroup: &RoleGroupRef<v1alpha1::AirflowCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    authentication_config: &AirflowClientAuthenticationDetailsResolved,
    authorization_config: &AirflowAuthorizationResolved,
    logging: &Logging<Container>,
    container: &Container,
) -> Result<ConfigMap, Error> {
    let mut config: BTreeMap<String, String> = BTreeMap::new();

    // this will call default values from AirflowClientAuthenticationDetails
    config::add_airflow_config(&mut config, authentication_config, authorization_config)
        .context(ConstructConfigSnafu)?;

    tracing::debug!(
        "Default config for {}: {:?}",
        rolegroup.object_name(),
        config
    );

    let mut file_config = rolegroup_config
        .get(&PropertyNameKind::File(AIRFLOW_CONFIG_FILENAME.to_string()))
        .cloned()
        .unwrap_or_default();

    tracing::debug!(
        "Config overrides for {}: {:?}",
        rolegroup.object_name(),
        file_config
    );

    // now add any overrides, replacing any defaults
    config.append(&mut file_config);

    tracing::debug!(
        "Merged config for {}: {:?}",
        rolegroup.object_name(),
        config
    );

    let mut config_file = Vec::new();

    // By removing the keys from `config_properties`, we avoid pasting the Python code into a Python variable as well
    // (which would be bad)
    if let Some(header) = config.remove(CONFIG_OVERRIDE_FILE_HEADER_KEY) {
        writeln!(config_file, "{}", header).context(WriteToConfigFileStringSnafu)?;
    }

    let temp_file_footer: Option<String> = config.remove(CONFIG_OVERRIDE_FILE_FOOTER_KEY);

    flask_app_config_writer::write::<AirflowConfigOptions, _, _>(
        &mut config_file,
        config.iter(),
        PYTHON_IMPORTS,
    )
    .with_context(|_| BuildRoleGroupConfigFileSnafu {
        rolegroup: rolegroup.clone(),
    })?;

    if let Some(footer) = temp_file_footer {
        writeln!(config_file, "{}", footer).context(WriteToConfigFileStringSnafu)?;
    }

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
    airflow: &v1alpha1::AirflowCluster,
    resolved_product_image: &ResolvedProductImage,
    rolegroup: &RoleGroupRef<v1alpha1::AirflowCluster>,
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
    airflow: &v1alpha1::AirflowCluster,
    resolved_product_image: &&ResolvedProductImage,
    rolegroup: &&RoleGroupRef<v1alpha1::AirflowCluster>,
    prometheus_label: Label,
) -> Result<ObjectMeta, Error> {
    let metadata = ObjectMetaBuilder::new()
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
        .with_label(prometheus_label)
        .build();
    Ok(metadata)
}

/// The rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
///
/// The [`Pod`](`stackable_operator::k8s_openapi::api::core::v1::Pod`)s are accessible through the corresponding [`Service`] (from [`build_rolegroup_service`]).
#[allow(clippy::too_many_arguments)]
fn build_server_rolegroup_statefulset(
    airflow: &v1alpha1::AirflowCluster,
    resolved_product_image: &ResolvedProductImage,
    airflow_role: &AirflowRole,
    rolegroup_ref: &RoleGroupRef<v1alpha1::AirflowCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    authentication_config: &AirflowClientAuthenticationDetailsResolved,
    authorization_config: &AirflowAuthorizationResolved,
    service_account: &ServiceAccount,
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
        .service_account_name(service_account.name_any())
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
    airflow_container_args.extend(airflow_role.get_commands(authentication_config));

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

    airflow_container.add_env_vars(
        env_vars::build_airflow_statefulset_envs(
            airflow,
            airflow_role,
            rolegroup_config,
            executor,
            authentication_config,
            authorization_config,
        )
        .context(BuildStatefulsetEnvVarsSnafu)?,
    );

    let volume_mounts = airflow.volume_mounts();
    airflow_container
        .add_volume_mounts(volume_mounts)
        .context(AddVolumeMountSnafu)?;
    airflow_container
        .add_volume_mount(CONFIG_VOLUME_NAME, CONFIG_PATH)
        .context(AddVolumeMountSnafu)?;
    airflow_container
        .add_volume_mount(LOG_CONFIG_VOLUME_NAME, LOG_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?;
    airflow_container
        .add_volume_mount(LOG_VOLUME_NAME, STACKABLE_LOG_DIR)
        .context(AddVolumeMountSnafu)?;

    if let AirflowExecutor::KubernetesExecutor { .. } = executor {
        airflow_container
            .add_volume_mount(TEMPLATE_VOLUME_NAME, TEMPLATE_LOCATION)
            .context(AddVolumeMountSnafu)?;
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
        .args(vec![
            [
                COMMON_BASH_TRAP_FUNCTIONS.to_string(),
                "prepare_signal_handlers".to_string(),
                "/stackable/statsd_exporter &".to_string(),
                "wait_for_termination $!".to_string(),
            ]
            .join("\n"),
        ])
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

    pb.add_volumes(airflow.volumes().clone())
        .context(AddVolumeSnafu)?;
    pb.add_volumes(controller_commons::create_volumes(
        &rolegroup_ref.object_name(),
        merged_airflow_config
            .logging
            .containers
            .get(&Container::Airflow),
    ))
    .context(AddVolumeSnafu)?;

    if let AirflowExecutor::KubernetesExecutor { .. } = executor {
        pb.add_volume(
            VolumeBuilder::new(TEMPLATE_VOLUME_NAME)
                .with_config_map(TEMPLATE_CONFIGMAP_NAME)
                .build(),
        )
        .context(AddVolumeSnafu)?;
    }

    if let Some(gitsync) = airflow.git_sync() {
        let gitsync_container = build_gitsync_container(
            resolved_product_image,
            &gitsync,
            false,
            &format!("{}-{}", GIT_SYNC_NAME, 1),
            build_gitsync_statefulset_envs(rolegroup_config, &gitsync.credentials_secret),
            airflow.volume_mounts(),
        )?;

        pb.add_volume(
            VolumeBuilder::new(GIT_SYNC_CONTENT)
                .empty_dir(EmptyDirVolumeSource::default())
                .build(),
        )
        .context(AddVolumeSnafu)?;

        pb.add_container(gitsync_container);

        if let AirflowExecutor::CeleryExecutor { .. } = executor {
            let gitsync_init_container = build_gitsync_container(
                resolved_product_image,
                &gitsync,
                true,
                &format!("{}-{}", GIT_SYNC_NAME, 0),
                build_gitsync_statefulset_envs(rolegroup_config, &gitsync.credentials_secret),
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
        match &airflow
            .spec
            .cluster_config
            .vector_aggregator_config_map_name
        {
            Some(vector_aggregator_config_map_name) => {
                pb.add_container(build_logging_container(
                    resolved_product_image,
                    merged_airflow_config
                        .logging
                        .containers
                        .get(&Container::Vector),
                    vector_aggregator_config_map_name,
                )?);
            }
            None => {
                VectorAggregatorConfigMapMissingSnafu.fail()?;
            }
        }
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
    vector_aggregator_config_map_name: &str,
) -> Result<k8s_openapi::api::core::v1::Container> {
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
        vector_aggregator_config_map_name,
    )
    .context(ConfigureLoggingSnafu)
}

#[allow(clippy::too_many_arguments)]
fn build_executor_template_config_map(
    airflow: &v1alpha1::AirflowCluster,
    resolved_product_image: &ResolvedProductImage,
    authentication_config: &AirflowClientAuthenticationDetailsResolved,
    sa_name: &str,
    merged_executor_config: &ExecutorConfig,
    env_overrides: &HashMap<String, String>,
    pod_overrides: &PodTemplateSpec,
    rolegroup_ref: &RoleGroupRef<v1alpha1::AirflowCluster>,
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
    // See https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/8.4.0/kubernetes_executor.html#base-image
    let mut airflow_container =
        ContainerBuilder::new(&Container::Base.to_string()).context(InvalidContainerNameSnafu)?;

    // Works too, had been changed
    add_authentication_volumes_and_volume_mounts(
        authentication_config,
        &mut airflow_container,
        &mut pb,
    )?;
    airflow_container
        .image_from_product_image(resolved_product_image)
        .resources(merged_executor_config.resources.clone().into())
        .add_env_vars(
            build_airflow_template_envs(airflow, env_overrides, merged_executor_config)
                .context(BuildStatefulsetEnvVarsSnafu)?,
        )
        .add_volume_mounts(airflow.volume_mounts())
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(CONFIG_VOLUME_NAME, CONFIG_PATH)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(LOG_CONFIG_VOLUME_NAME, LOG_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(LOG_VOLUME_NAME, STACKABLE_LOG_DIR)
        .context(AddVolumeMountSnafu)?;

    pb.add_container(airflow_container.build());
    pb.add_volumes(airflow.volumes().clone())
        .context(AddVolumeSnafu)?;
    pb.add_volumes(controller_commons::create_volumes(
        &rolegroup_ref.object_name(),
        merged_executor_config
            .logging
            .containers
            .get(&Container::Airflow),
    ))
    .context(AddVolumeSnafu)?;

    if let Some(gitsync) = airflow.git_sync() {
        let gitsync_container = build_gitsync_container(
            resolved_product_image,
            &gitsync,
            true,
            &format!("{}-{}", GIT_SYNC_NAME, 0),
            build_gitsync_template(env_overrides, &gitsync.credentials_secret),
            airflow.volume_mounts(),
        )?;
        pb.add_volume(
            VolumeBuilder::new(GIT_SYNC_CONTENT)
                .empty_dir(EmptyDirVolumeSource::default())
                .build(),
        )
        .context(AddVolumeSnafu)?;
        pb.add_init_container(gitsync_container);
    }

    if merged_executor_config.logging.enable_vector_agent {
        match &airflow
            .spec
            .cluster_config
            .vector_aggregator_config_map_name
        {
            Some(vector_aggregator_config_map_name) => {
                pb.add_container(build_logging_container(
                    resolved_product_image,
                    merged_executor_config
                        .logging
                        .containers
                        .get(&Container::Vector),
                    vector_aggregator_config_map_name,
                )?);
            }
            None => {
                VectorAggregatorConfigMapMissingSnafu.fail()?;
            }
        }
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
        .add_volume_mount(GIT_SYNC_CONTENT, GIT_SYNC_ROOT)
        .context(AddVolumeMountSnafu)?
        .add_volume_mounts(volume_mounts)
        .context(AddVolumeMountSnafu)?
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

pub fn error_policy(
    _obj: Arc<DeserializeGuard<v1alpha1::AirflowCluster>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> Action {
    match error {
        // root object is invalid, will be requeued when modified anyway
        Error::InvalidAirflowCluster { .. } => Action::await_change(),

        _ => Action::requeue(*Duration::from_secs(10)),
    }
}
// I want to add secret volumes right here

fn add_authentication_volumes_and_volume_mounts(
    authentication_config: &AirflowClientAuthenticationDetailsResolved,
    cb: &mut ContainerBuilder,
    pb: &mut PodBuilder,
) -> Result<()> {
    // Different authentication entries can reference the same secret
    // class or TLS certificate. It must be ensured that the volumes
    // and volume mounts are only added once in such a case.

    let mut ldap_authentication_providers = BTreeSet::new();
    let mut tls_client_credentials = BTreeSet::new();

    for auth_class_resolved in &authentication_config.authentication_classes_resolved {
        match auth_class_resolved {
            AirflowAuthenticationClassResolved::Ldap { provider } => {
                ldap_authentication_providers.insert(provider);
            }
            AirflowAuthenticationClassResolved::Oidc { provider, .. } => {
                tls_client_credentials.insert(&provider.tls);
            }
        }
    }

    for provider in ldap_authentication_providers {
        provider
            .add_volumes_and_mounts(pb, vec![cb])
            .context(AddLdapVolumesAndVolumeMountsSnafu)?;
    }

    for tls in tls_client_credentials {
        tls.add_volumes_and_mounts(pb, vec![cb])
            .context(AddTlsVolumesAndVolumeMountsSnafu)?;
    }
    Ok(())
}
