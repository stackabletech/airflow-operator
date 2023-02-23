use super::AIRFLOW_CONTROLLER_NAME;
use crate::common::config::{self, PYTHON_IMPORTS};
use crate::common::controller_commons::{
    CONFIG_VOLUME_NAME, LOG_CONFIG_VOLUME_NAME, LOG_VOLUME_NAME,
};

use crate::common::product_logging::extend_config_map_with_log_config;
use crate::common::util::env_var_from_secret;
use crate::common::{controller_commons, rbac};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_airflow_crd::{
    build_recommended_labels, AirflowCluster, AirflowConfig, AirflowConfigOptions, AirflowRole,
    Container, AIRFLOW_CONFIG_FILENAME, APP_NAME, CONFIG_PATH, LOG_CONFIG_DIR, STACKABLE_LOG_DIR,
};
use stackable_operator::{
    builder::{
        ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder,
        PodSecurityContextBuilder,
    },
    commons::{
        authentication::{AuthenticationClass, AuthenticationClassProvider},
        product_image_selection::ResolvedProductImage,
    },
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, EnvVar, Probe, Service, ServicePort, ServiceSpec, TCPSocketAction,
            },
        },
        apimachinery::pkg::{apis::meta::v1::LabelSelector, util::intstr::IntOrString},
    },
    kube::{runtime::reflector::ObjectRef, ResourceExt},
    labels::{role_group_selector_labels, role_selector_labels},
    product_config::{
        flask_app_config_writer, flask_app_config_writer::FlaskAppConfigWriterError,
        types::PropertyNameKind,
    },
    product_logging::{self, spec::Logging},
    role_utils::RoleGroupRef,
};
use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
};
use strum::{EnumDiscriminants, IntoStaticStr};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object defines no airflow config role"))]
    NoAirflowRole,
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
    },
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
    #[snafu(display("invalid container name"))]
    InvalidContainerName {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to add the logging configuration to the ConfigMap [{cm_name}]"))]
    InvalidLoggingConfig {
        source: crate::common::product_logging::Error,
        cm_name: String,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

const METRICS_PORT_NAME: &str = "metrics";
const METRICS_PORT: i32 = 9102;

/// The server-role service is the primary endpoint that should be used by clients that do not perform internal load balancing,
/// including targets outside of the cluster.
pub fn build_role_service(
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

pub fn role_port(role_name: &str) -> Option<u16> {
    AirflowRole::from_str(role_name).unwrap().get_http_port()
}

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the administrator
pub fn build_rolegroup_config_map(
    airflow: &AirflowCluster,
    resolved_product_image: &ResolvedProductImage,
    rolegroup: &RoleGroupRef<AirflowCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    authentication_class: Option<&AuthenticationClass>,
    logging: &Logging<Container>,
    vector_aggregator_address: Option<&str>,
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
        &Container::Airflow,
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
pub fn build_rolegroup_service(
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
pub fn build_server_rolegroup_statefulset(
    airflow: &AirflowCluster,
    resolved_product_image: &ResolvedProductImage,
    rolegroup_ref: &RoleGroupRef<AirflowCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    authentication_class: Option<&AuthenticationClass>,
    sa_name: &str,
    config: &AirflowConfig,
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
    let mut cb = ContainerBuilder::new(&Container::Airflow.to_string())
        .context(InvalidContainerNameSnafu)?;
    let mut pb = PodBuilder::new();

    if let Some(authentication_class) = authentication_class {
        add_authentication_volumes_and_volume_mounts(authentication_class, &mut cb, &mut pb)?;
    }

    let cb = cb
        .image_from_product_image(resolved_product_image)
        .resources(config.resources.clone().into())
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
    cb.add_volume_mount(CONFIG_VOLUME_NAME, CONFIG_PATH);
    cb.add_volume_mount(LOG_CONFIG_VOLUME_NAME, LOG_CONFIG_DIR);
    cb.add_volume_mount(LOG_VOLUME_NAME, STACKABLE_LOG_DIR);

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
        .context(InvalidContainerNameSnafu)?
        .image_from_product_image(resolved_product_image)
        .command(vec!["/bin/bash".to_string(), "-c".to_string()])
        .args(vec!["/stackable/statsd_exporter".to_string()])
        .add_container_port(METRICS_PORT_NAME, METRICS_PORT)
        .build();

    let mut volumes = airflow.volumes();
    volumes.extend(controller_commons::create_volumes(
        &rolegroup_ref.object_name(),
        config.logging.containers.get(&Container::Airflow),
    ));

    pb.add_container(container);
    pb.add_container(metrics_container);

    if config.logging.enable_vector_agent {
        pb.add_container(product_logging::framework::vector_container(
            resolved_product_image,
            CONFIG_VOLUME_NAME,
            LOG_VOLUME_NAME,
            config.logging.containers.get(&Container::Vector),
        ));
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

fn add_authentication_volumes_and_volume_mounts(
    authentication_class: &AuthenticationClass,
    cb: &mut ContainerBuilder,
    pb: &mut PodBuilder,
) -> Result<()> {
    match &authentication_class.spec.provider {
        AuthenticationClassProvider::Ldap(ldap) => {
            ldap.add_volumes_and_mounts(pb, vec![cb]);
            Ok(())
        }
        _ => AuthenticationClassProviderNotSupportedSnafu {
            authentication_class_provider: authentication_class.spec.provider.to_string(),
            authentication_class: ObjectRef::<AuthenticationClass>::new(
                &authentication_class.name_unchecked(),
            ),
        }
        .fail(),
    }
}
