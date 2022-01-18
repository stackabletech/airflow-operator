//! Ensures that `Pod`s are configured and running for each [`AirflowCluster`]

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    time::Duration,
};

use crate::{APP_NAME, APP_PORT};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_airflow_crd::{AirflowCluster, AirflowConfig, AirflowRole};
use stackable_operator::{
    builder::{ContainerBuilder, ObjectMetaBuilder, PodBuilder},
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                EnvVar, EnvVarSource, SecretKeySelector, Service, ServicePort, ServiceSpec,
            },
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
    },
    kube::runtime::controller::{Context, ReconcilerAction},
    labels::{role_group_selector_labels, role_selector_labels},
    product_config::{types::PropertyNameKind, ProductConfigManager},
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    role_utils::RoleGroupRef,
};

const FIELD_MANAGER_SCOPE: &str = "airflowcluster";

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
}

#[derive(Snafu, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object defines no version"))]
    ObjectHasNoVersion,
    #[snafu(display("object defines no node role"))]
    NoNodeRole,
    #[snafu(display("failed to calculate global service name"))]
    GlobalServiceNameNotFound,
    #[snafu(display("failed to apply global Service"))]
    ApplyRoleService {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to apply Service for {}", rolegroup))]
    ApplyRoleGroupService {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<AirflowCluster>,
    },
    #[snafu(display("failed to apply StatefulSet for {}", rolegroup))]
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
}
type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn reconcile_worker(
    airflow: AirflowCluster,
    ctx: Context<Ctx>,
) -> Result<ReconcilerAction> {
    tracing::info!("Starting reconcile");

    let client = &ctx.get_ref().client;

    let role_config = &transform_all_roles_to_config(
        &airflow,
        [(
            AirflowRole::Node.to_string(),
            (
                vec![PropertyNameKind::Env],
                airflow.spec.nodes.clone().context(NoNodeRole)?,
            ),
        )]
        .into(),
    );
    let validated_role_config = validate_all_roles_and_groups_config(
        airflow_version(&airflow)?,
        role_config,
        &ctx.get_ref().product_config,
        false,
        false,
    )
    .context(InvalidProductConfig)?;

    tracing::info!("validated_config {:?}", &validated_role_config);

    let role_node_config = validated_role_config
        .get(&AirflowRole::Node.to_string())
        .map(Cow::Borrowed)
        .unwrap_or_default();

    let node_role_service = build_node_role_service(&airflow)?;

    client
        .apply_patch(FIELD_MANAGER_SCOPE, &node_role_service, &node_role_service)
        .await
        .context(ApplyRoleService)?;
    for (rolegroup_name, rolegroup_config) in role_node_config.iter() {
        let rolegroup = airflow.node_rolegroup_ref(rolegroup_name);

        let rg_service = build_node_rolegroup_service(&rolegroup, &airflow)?;
        let rg_statefulset =
            build_server_rolegroup_statefulset(&rolegroup, &airflow, rolegroup_config)?;
        client
            .apply_patch(FIELD_MANAGER_SCOPE, &rg_service, &rg_service)
            .await
            .with_context(|| ApplyRoleGroupService {
                rolegroup: rolegroup.clone(),
            })?;
        client
            .apply_patch(FIELD_MANAGER_SCOPE, &rg_statefulset, &rg_statefulset)
            .await
            .with_context(|| ApplyRoleGroupStatefulSet {
                rolegroup: rolegroup.clone(),
            })?;
    }

    Ok(ReconcilerAction {
        requeue_after: None,
    })
}

/// The server-role service is the primary endpoint that should be used by clients that do not perform internal load balancing,
/// including targets outside of the cluster.
pub fn build_node_role_service(airflow: &AirflowCluster) -> Result<Service> {
    let role_name = AirflowRole::Node.to_string();
    let role_svc_name = airflow
        .node_role_service_name()
        .context(GlobalServiceNameNotFound)?;
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(airflow)
            .name(format!("{}-external", &role_svc_name))
            .ownerreference_from_resource(airflow, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRef)?
            .with_recommended_labels(
                airflow,
                APP_NAME,
                airflow_version(airflow)?,
                &role_name,
                "global",
            )
            .build(),
        spec: Some(ServiceSpec {
            ports: Some(vec![ServicePort {
                name: Some("airflow".to_string()),
                port: APP_PORT.into(),
                protocol: Some("TCP".to_string()),
                ..ServicePort::default()
            }]),
            selector: Some(role_selector_labels(airflow, APP_NAME, &role_name)),
            type_: Some("NodePort".to_string()),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

/// The rolegroup [`Service`] is a headless service that allows direct access to the instances of a certain rolegroup
///
/// This is mostly useful for internal communication between peers, or for clients that perform client-side load balancing.
fn build_node_rolegroup_service(
    rolegroup: &RoleGroupRef<AirflowCluster>,
    airflow: &AirflowCluster,
) -> Result<Service> {
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(airflow)
            .name(&rolegroup.object_name())
            .ownerreference_from_resource(airflow, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRef)?
            .with_recommended_labels(
                airflow,
                APP_NAME,
                airflow_version(airflow)?,
                &rolegroup.role,
                &rolegroup.role_group,
            )
            .build(),
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            ports: Some(vec![ServicePort {
                name: Some("airflow".to_string()),
                port: APP_PORT.into(),
                protocol: Some("TCP".to_string()),
                ..ServicePort::default()
            }]),
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
    rolegroup_ref: &RoleGroupRef<AirflowCluster>,
    airflow: &AirflowCluster,
    node_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<StatefulSet> {
    let rolegroup = airflow
        .spec
        .nodes
        .as_ref()
        .context(NoNodeRole)?
        .role_groups
        .get(&rolegroup_ref.role_group);

    let airflow_version = airflow_version(airflow)?;

    /*let image = format!(
        "docker.stackable.tech/stackable/airflow:{}-stackable0",
        airflow_version
    );*/
    let image = "apache/airflow:2.2.3";

    // environment variables
    let env = build_envs(airflow, node_config);

    // initialising commands
    let commands = build_commands();

    let container = ContainerBuilder::new("airflow-worker")
        .image(image)
        .command(vec!["/bin/bash".to_string()])
        .args(vec![String::from("-c"), commands.join("; ")])
        .add_env_vars(env)
        .add_container_port("http", APP_PORT.into())
        .build();

    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(airflow)
            .name(&rolegroup_ref.object_name())
            .ownerreference_from_resource(airflow, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRef)?
            .with_recommended_labels(
                airflow,
                APP_NAME,
                airflow_version,
                &rolegroup_ref.role,
                &rolegroup_ref.role_group,
            )
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
            template: PodBuilder::new()
                .metadata_builder(|m| {
                    m.with_recommended_labels(
                        airflow,
                        APP_NAME,
                        airflow_version,
                        &rolegroup_ref.role,
                        &rolegroup_ref.role_group,
                    )
                })
                .add_container(container)
                .build_template(),
            ..StatefulSetSpec::default()
        }),
        status: None,
    })
}

fn build_envs(
    airflow: &AirflowCluster,
    node_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Vec<EnvVar> {
    let secret_prop = node_config
        .get(&PropertyNameKind::Env)
        .and_then(|vars| vars.get(AirflowConfig::CREDENTIALS_SECRET_PROPERTY));

    let mut env = secret_prop
        .map(|secret| {
            vec![
                env_var_from_secret("SECRET_KEY", secret, "connections.secretKey"),
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
                env_var_from_secret("ADMIN_USERNAME", secret, "adminUser.username"),
                env_var_from_secret("ADMIN_FIRSTNAME", secret, "adminUser.firstname"),
                env_var_from_secret("ADMIN_LASTNAME", secret, "adminUser.lastname"),
                env_var_from_secret("ADMIN_EMAIL", secret, "adminUser.email"),
                env_var_from_secret("ADMIN_PASSWORD", secret, "adminUser.password"),
            ]
        })
        .unwrap_or_default();

    if airflow.spec.load_examples.unwrap_or_default() {
        env.push(EnvVar {
            name: String::from("AIRFLOW__CORE__LOAD_EXAMPLES"),
            value: Some(String::from("true")),
            value_from: None,
        })
    }

    if airflow.spec.expose_config.unwrap_or_default() {
        env.push(EnvVar {
            name: String::from("AIRFLOW__WEBSERVER__EXPOSE_CONFIG"),
            value: Some(String::from("true")),
            value_from: None,
        })
    }

    let executor = airflow.spec.executor.clone();

    env.push(EnvVar {
        name: String::from("AIRFLOW__CORE__EXECUTOR"),
        value: executor,
        value_from: None,
    });
    env
}

fn build_commands() -> Vec<String> {
    vec![
        String::from("airflow celery worker"),
    ]
}

pub fn airflow_version(airflow: &AirflowCluster) -> Result<&str> {
    airflow.spec.version.as_deref().context(ObjectHasNoVersion)
}

pub fn error_policy(_error: &Error, _ctx: Context<Ctx>) -> ReconcilerAction {
    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(5)),
    }
}

fn env_var_from_secret(var_name: &str, secret: &str, secret_key: &str) -> EnvVar {
    EnvVar {
        name: String::from(var_name),
        value_from: Some(EnvVarSource {
            secret_key_ref: Some(SecretKeySelector {
                name: Some(String::from(secret)),
                key: String::from(secret_key),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use stackable_airflow_crd::AirflowCluster;

    #[test]
    fn test_cluster_config() {
        let cluster: AirflowCluster = serde_yaml::from_str::<AirflowCluster>(
            "
        apiVersion: airflow.stackable.tech/v1alpha1
        kind: AirflowCluster
        metadata:
          name: airflow
        spec:
          version: 1.3.2
          executor: KubernetesExecutor
          loadExamples: true
          exposeConfig: true
          nodes:
            roleGroups:
              default:
                config:
                  credentialsSecret: simple-airflow-credentials

          ",
        )
        .unwrap();

        assert_eq!("1.3.2", cluster.spec.version.unwrap_or_default());
        assert_eq!(
            "KubernetesExecutor",
            cluster.spec.executor.unwrap_or_default()
        );
        assert_eq!(true, cluster.spec.load_examples.unwrap_or(false));
        assert_eq!(true, cluster.spec.expose_config.unwrap_or(false));
    }
}
