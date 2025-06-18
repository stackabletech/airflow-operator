use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::PathBuf,
};

use product_config::types::PropertyNameKind;
use snafu::Snafu;
use stackable_operator::{
    commons::product_image_selection::ResolvedProductImage,
    crd::{authentication::oidc, git_sync},
    k8s_openapi::api::core::v1::EnvVar,
    kube::ResourceExt,
    product_logging::framework::create_vector_shutdown_file_command,
};

use crate::{
    crd::{
        AirflowConfig, AirflowExecutor, AirflowRole, ExecutorConfig, LOG_CONFIG_DIR,
        STACKABLE_LOG_DIR, TEMPLATE_LOCATION, TEMPLATE_NAME,
        authentication::{
            AirflowAuthenticationClassResolved, AirflowClientAuthenticationDetailsResolved,
        },
        authorization::AirflowAuthorizationResolved,
        v1alpha1,
    },
    util::env_var_from_secret,
};

const AIRFLOW_CORE_AUTH_MANAGER: &str = "AIRFLOW__CORE__AUTH_MANAGER";
const AIRFLOW_LOGGING_LOGGING_CONFIG_CLASS: &str = "AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS";
const AIRFLOW_METRICS_STATSD_ON: &str = "AIRFLOW__METRICS__STATSD_ON";
const AIRFLOW_METRICS_STATSD_HOST: &str = "AIRFLOW__METRICS__STATSD_HOST";
const AIRFLOW_METRICS_STATSD_PORT: &str = "AIRFLOW__METRICS__STATSD_PORT";
const AIRFLOW_WEBSERVER_SECRET_KEY: &str = "AIRFLOW__WEBSERVER__SECRET_KEY";
const AIRFLOW_CELERY_RESULT_BACKEND: &str = "AIRFLOW__CELERY__RESULT_BACKEND";
const AIRFLOW_CELERY_BROKER_URL: &str = "AIRFLOW__CELERY__BROKER_URL";
const AIRFLOW_CORE_DAGS_FOLDER: &str = "AIRFLOW__CORE__DAGS_FOLDER";
const AIRFLOW_CORE_LOAD_EXAMPLES: &str = "AIRFLOW__CORE__LOAD_EXAMPLES";
const AIRFLOW_API_AUTH_BACKENDS: &str = "AIRFLOW__API__AUTH_BACKENDS";
const AIRFLOW_DATABASE_SQL_ALCHEMY_CONN: &str = "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN";

const AIRFLOW_WEBSERVER_EXPOSE_CONFIG: &str = "AIRFLOW__WEBSERVER__EXPOSE_CONFIG";
const AIRFLOW_CORE_EXECUTOR: &str = "AIRFLOW__CORE__EXECUTOR";
const AIRFLOW_KUBERNETES_EXECUTOR_POD_TEMPLATE_FILE: &str =
    "AIRFLOW__KUBERNETES_EXECUTOR__POD_TEMPLATE_FILE";
const AIRFLOW_KUBERNETES_EXECUTOR_NAMESPACE: &str = "AIRFLOW__KUBERNETES_EXECUTOR__NAMESPACE";

const ADMIN_FIRSTNAME: &str = "ADMIN_FIRSTNAME";
const ADMIN_USERNAME: &str = "ADMIN_USERNAME";
const ADMIN_LASTNAME: &str = "ADMIN_LASTNAME";
const ADMIN_PASSWORD: &str = "ADMIN_PASSWORD";
const ADMIN_EMAIL: &str = "ADMIN_EMAIL";

const PYTHONPATH: &str = "PYTHONPATH";

/// This key is only intended for use during experimental support and will
/// be replaced with a secret at a later stage. See the issue covering
/// this at <https://github.com/stackabletech/airflow-operator/issues/639>.
const JWT_KEY: &str = "ThisKeyIsNotIntendedForProduction!";

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display(
        "failed to construct Git DAG folder - Is the git folder a valid path?: {dag_folder:?}"
    ))]
    ConstructGitDagFolder { dag_folder: PathBuf },
}

/// Return environment variables to be applied to the statefulsets for the scheduler, webserver (and worker,
/// for clusters utilizing `celeryExecutor`: for clusters using `kubernetesExecutor` a different set will be
/// used which is defined in [`build_airflow_template_envs`]).
#[allow(clippy::too_many_arguments)]
pub fn build_airflow_statefulset_envs(
    airflow: &v1alpha1::AirflowCluster,
    airflow_role: &AirflowRole,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    executor: &AirflowExecutor,
    auth_config: &AirflowClientAuthenticationDetailsResolved,
    authorization_config: &AirflowAuthorizationResolved,
    git_sync_resources: &git_sync::v1alpha1::GitSyncResources,
    resolved_product_image: &ResolvedProductImage,
) -> Result<Vec<EnvVar>, Error> {
    let mut env: BTreeMap<String, EnvVar> = BTreeMap::new();

    env.extend(static_envs(git_sync_resources));

    add_version_specific_env_vars(airflow, airflow_role, resolved_product_image, &mut env);

    // environment variables
    let env_vars = rolegroup_config.get(&PropertyNameKind::Env);

    let secret_prop =
        env_vars.and_then(|vars| vars.get(AirflowConfig::CREDENTIALS_SECRET_PROPERTY));

    if let Some(secret) = secret_prop {
        env.insert(
            AIRFLOW_WEBSERVER_SECRET_KEY.into(),
            // The secret key is used to run the webserver flask app and also used to authorize
            // requests to Celery workers when logs are retrieved.
            env_var_from_secret(
                AIRFLOW_WEBSERVER_SECRET_KEY,
                secret,
                "connections.secretKey",
            ),
        );
        env.insert(
            AIRFLOW_DATABASE_SQL_ALCHEMY_CONN.into(),
            env_var_from_secret(
                AIRFLOW_DATABASE_SQL_ALCHEMY_CONN,
                secret,
                "connections.sqlalchemyDatabaseUri",
            ),
        );

        // Redis is only needed when celery executors are used
        // see https://github.com/stackabletech/airflow-operator/issues/424 for details
        if matches!(executor, AirflowExecutor::CeleryExecutor { .. }) {
            env.insert(
                AIRFLOW_CELERY_RESULT_BACKEND.into(),
                env_var_from_secret(
                    AIRFLOW_CELERY_RESULT_BACKEND,
                    secret,
                    "connections.celeryResultBackend",
                ),
            );
            env.insert(
                AIRFLOW_CELERY_BROKER_URL.into(),
                env_var_from_secret(
                    AIRFLOW_CELERY_BROKER_URL,
                    secret,
                    "connections.celeryBrokerUrl",
                ),
            );
        }
    }

    let dags_folder = get_dags_folder(git_sync_resources);
    env.insert(
        AIRFLOW_CORE_DAGS_FOLDER.into(),
        EnvVar {
            name: AIRFLOW_CORE_DAGS_FOLDER.into(),
            value: Some(dags_folder),
            ..Default::default()
        },
    );

    if airflow.spec.cluster_config.load_examples {
        env.insert(
            AIRFLOW_CORE_LOAD_EXAMPLES.into(),
            EnvVar {
                name: AIRFLOW_CORE_LOAD_EXAMPLES.into(),
                value: Some("True".into()),
                ..Default::default()
            },
        );
    } else {
        env.insert(
            AIRFLOW_CORE_LOAD_EXAMPLES.into(),
            EnvVar {
                name: AIRFLOW_CORE_LOAD_EXAMPLES.into(),
                value: Some("False".into()),
                ..Default::default()
            },
        );
    }

    if airflow.spec.cluster_config.expose_config {
        env.insert(
            AIRFLOW_WEBSERVER_EXPOSE_CONFIG.into(),
            EnvVar {
                name: AIRFLOW_WEBSERVER_EXPOSE_CONFIG.into(),
                value: Some("True".into()),
                ..Default::default()
            },
        );
    }

    env.insert(
        AIRFLOW_CORE_EXECUTOR.into(),
        EnvVar {
            name: AIRFLOW_CORE_EXECUTOR.into(),
            value: Some(executor.to_string()),
            ..Default::default()
        },
    );

    if let AirflowExecutor::KubernetesExecutor { .. } = executor {
        env.insert(
            AIRFLOW_KUBERNETES_EXECUTOR_POD_TEMPLATE_FILE.into(),
            EnvVar {
                name: AIRFLOW_KUBERNETES_EXECUTOR_POD_TEMPLATE_FILE.into(),
                value: Some(format!("{TEMPLATE_LOCATION}/{TEMPLATE_NAME}")),
                ..Default::default()
            },
        );
        env.insert(
            AIRFLOW_KUBERNETES_EXECUTOR_NAMESPACE.into(),
            EnvVar {
                name: AIRFLOW_KUBERNETES_EXECUTOR_NAMESPACE.into(),
                value: airflow.namespace(),
                ..Default::default()
            },
        );
    }

    match airflow_role {
        // Database initialization is limited to the scheduler.
        // See https://github.com/stackabletech/airflow-operator/issues/259
        AirflowRole::Scheduler => {
            let secret = &airflow.spec.cluster_config.credentials_secret;
            env.insert(
                ADMIN_USERNAME.into(),
                env_var_from_secret(ADMIN_USERNAME, secret, "adminUser.username"),
            );
            env.insert(
                ADMIN_FIRSTNAME.into(),
                env_var_from_secret(ADMIN_FIRSTNAME, secret, "adminUser.firstname"),
            );
            env.insert(
                ADMIN_LASTNAME.into(),
                env_var_from_secret(ADMIN_LASTNAME, secret, "adminUser.lastname"),
            );
            env.insert(
                ADMIN_EMAIL.into(),
                env_var_from_secret(ADMIN_EMAIL, secret, "adminUser.email"),
            );
            env.insert(
                ADMIN_PASSWORD.into(),
                env_var_from_secret(ADMIN_PASSWORD, secret, "adminUser.password"),
            );
        }
        AirflowRole::Webserver => {
            let mut vars = authentication_env_vars(auth_config);
            vars.extend(authorization_env_vars(authorization_config));
            env.extend(vars.into_iter().map(|var| (var.name.to_owned(), var)));
        }
        _ => {}
    }

    // apply overrides last of all with a fixed ordering
    if let Some(env_vars) = env_vars {
        for (k, v) in env_vars.iter().collect::<BTreeMap<_, _>>() {
            env.insert(
                k.into(),
                EnvVar {
                    name: k.to_string(),
                    value: Some(v.to_string()),
                    ..Default::default()
                },
            );
        }
    }

    // Needed for the `containerdebug` process to log it's tracing information to.
    env.insert(
        "CONTAINERDEBUG_LOG_DIRECTORY".to_string(),
        EnvVar {
            name: "CONTAINERDEBUG_LOG_DIRECTORY".to_string(),
            value: Some(format!("{STACKABLE_LOG_DIR}/containerdebug")),
            value_from: None,
        },
    );

    tracing::debug!("Env-var set [{:?}]", env);
    Ok(transform_map_to_vec(env))
}

pub fn get_dags_folder(git_sync_resources: &git_sync::v1alpha1::GitSyncResources) -> String {
    let git_sync_count = git_sync_resources.git_content_folders.len();
    if git_sync_count > 1 {
        tracing::warn!(
            "There are {git_sync_count} git-sync entries: Only the first one will be considered.",
        );
    }

    // If DAG provisioning via git-sync is not configured, set a default value
    // so that PYTHONPATH can refer to it. N.B. nested variables need to be
    // resolved, so that /stackable/airflow is used instead of $AIRFLOW_HOME.
    // see https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#dags-folder
    git_sync_resources
        .git_content_folders_as_string()
        .first()
        .cloned()
        .unwrap_or("/stackable/airflow/dags".to_string())
}

// This set of environment variables is a standard set that is not dependent on any
// conditional logic and should be applied to the statefulset or the executor template config map.
fn static_envs(
    git_sync_resources: &git_sync::v1alpha1::GitSyncResources,
) -> BTreeMap<String, EnvVar> {
    let mut env: BTreeMap<String, EnvVar> = BTreeMap::new();

    let dags_folder = get_dags_folder(git_sync_resources);

    env.insert(
        PYTHONPATH.into(),
        EnvVar {
            // PYTHONPATH must be extended to include the dags folder so that dag
            // dependencies can be found: this must be the actual path and not a variable.
            // Also include the airflow site-packages by default (for airflow and kubernetes classes etc.)
            name: PYTHONPATH.into(),
            value: Some(format!("{LOG_CONFIG_DIR}:{dags_folder}")),
            ..Default::default()
        },
    );
    env.insert(
        AIRFLOW_LOGGING_LOGGING_CONFIG_CLASS.into(),
        EnvVar {
            name: AIRFLOW_LOGGING_LOGGING_CONFIG_CLASS.into(),
            value: Some("log_config.LOGGING_CONFIG".into()),
            ..Default::default()
        },
    );

    env.insert(
        AIRFLOW_METRICS_STATSD_ON.into(),
        EnvVar {
            name: AIRFLOW_METRICS_STATSD_ON.into(),
            value: Some("True".into()),
            ..Default::default()
        },
    );

    env.insert(
        AIRFLOW_METRICS_STATSD_HOST.into(),
        EnvVar {
            name: AIRFLOW_METRICS_STATSD_HOST.into(),
            value: Some("0.0.0.0".into()),
            ..Default::default()
        },
    );

    env.insert(
        AIRFLOW_METRICS_STATSD_PORT.into(),
        EnvVar {
            name: AIRFLOW_METRICS_STATSD_PORT.into(),
            value: Some("9125".into()),
            ..Default::default()
        },
    );

    env
}

/// Return environment variables to be applied to the configuration map used in conjunction with
/// the `kubernetesExecutor` worker.
pub fn build_airflow_template_envs(
    airflow: &v1alpha1::AirflowCluster,
    env_overrides: &HashMap<String, String>,
    config: &ExecutorConfig,
    git_sync_resources: &git_sync::v1alpha1::GitSyncResources,
    resolved_product_image: &ResolvedProductImage,
) -> Vec<EnvVar> {
    let mut env: BTreeMap<String, EnvVar> = BTreeMap::new();
    let secret = airflow.spec.cluster_config.credentials_secret.as_str();

    env.insert(
        AIRFLOW_DATABASE_SQL_ALCHEMY_CONN.into(),
        env_var_from_secret(
            AIRFLOW_DATABASE_SQL_ALCHEMY_CONN,
            secret,
            "connections.sqlalchemyDatabaseUri",
        ),
    );

    env.insert(
        AIRFLOW_CORE_EXECUTOR.into(),
        EnvVar {
            name: AIRFLOW_CORE_EXECUTOR.into(),
            value: Some("LocalExecutor".to_string()),
            ..Default::default()
        },
    );

    env.insert(
        AIRFLOW_KUBERNETES_EXECUTOR_NAMESPACE.into(),
        EnvVar {
            name: AIRFLOW_KUBERNETES_EXECUTOR_NAMESPACE.into(),
            value: airflow.namespace(),
            ..Default::default()
        },
    );

    // the config map also requires the dag-folder location as this will be passed on
    // to the pods started by airflow.
    let dags_folder = get_dags_folder(git_sync_resources);
    env.insert(
        AIRFLOW_CORE_DAGS_FOLDER.into(),
        EnvVar {
            name: AIRFLOW_CORE_DAGS_FOLDER.into(),
            value: Some(dags_folder),
            ..Default::default()
        },
    );

    env.extend(static_envs(git_sync_resources));

    add_version_specific_env_vars(
        airflow,
        &AirflowRole::Worker,
        resolved_product_image,
        &mut env,
    );

    // _STACKABLE_POST_HOOK will contain a command to create a shutdown hook that will be
    // evaluated in the wrapper for each stackable spark container: this is necessary for pods
    // that are created and then terminated (we do a similar thing for spark-k8s).
    if config.logging.enable_vector_agent {
        env.insert(
            "_STACKABLE_POST_HOOK".into(),
            EnvVar {
                name: "_STACKABLE_POST_HOOK".into(),
                value: Some(
                    [
                        // Wait for Vector to gather the logs.
                        "sleep 10",
                        &create_vector_shutdown_file_command(STACKABLE_LOG_DIR),
                    ]
                    .join("; "),
                ),
                ..Default::default()
            },
        );
    }

    // iterate over a BTreeMap to ensure the vars are written in a predictable order
    for (k, v) in env_overrides.iter().collect::<BTreeMap<_, _>>() {
        env.insert(
            k.to_string(),
            EnvVar {
                name: k.to_string(),
                value: Some(v.to_string()),
                ..Default::default()
            },
        );
    }

    tracing::debug!("Env-var set [{:?}]", env);
    transform_map_to_vec(env)
}

fn add_version_specific_env_vars(
    airflow: &v1alpha1::AirflowCluster,
    airflow_role: &AirflowRole,
    resolved_product_image: &ResolvedProductImage,
    env: &mut BTreeMap<String, EnvVar>,
) {
    if resolved_product_image.product_version.starts_with("3.") {
        env.extend(execution_server_env_vars(airflow));
        env.insert(
            AIRFLOW_CORE_AUTH_MANAGER.into(),
            EnvVar {
                name: AIRFLOW_CORE_AUTH_MANAGER.into(),
                value: Some(
                    "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"
                        .to_string(),
                ),
                ..Default::default()
            },
        );
        env.insert(
            AIRFLOW_API_AUTH_BACKENDS.into(),
            EnvVar {
                name: AIRFLOW_API_AUTH_BACKENDS.into(),
                value: Some("airflow.api.auth.backend.session".into()),
                ..Default::default()
            },
        );
        // As of 3.x a JWT key is required.
        // See https://airflow.apache.org/docs/apache-airflow/3.0.1/configurations-ref.html#jwt-secret
        // This should be random, but must also be consistent across
        // api-services and replicas/roles for a given
        // cluster, but should also be cluster-specific.
        // See issue <https://github.com/stackabletech/airflow-operator/issues/639>:
        // later it will be accessed from a secret to avoid cluster restarts
        // being triggered by an operator restart.
        env.insert(
            "AIRFLOW__API_AUTH__JWT_SECRET".into(),
            EnvVar {
                name: "AIRFLOW__API_AUTH__JWT_SECRET".into(),
                value: Some(JWT_KEY.into()),
                ..Default::default()
            },
        );
        if airflow_role == &AirflowRole::Webserver {
            // Sometimes a race condition can arise when both scheduler and
            // api-server are updating the DB, which adds overhead (conflicts
            // are logged) and can result in inconsistencies. This setting
            // ensure that only the scheduler will do this by default.
            env.insert(
                "AIRFLOW__FAB__UPDATE_FAB_PERMS".into(),
                EnvVar {
                    name: "AIRFLOW__FAB__UPDATE_FAB_PERMS".into(),
                    value: Some("False".into()),
                    ..Default::default()
                },
            );
            // Airflow 3.x uses fast-api as a backend: newer versions of uvicorn can
            // cause issues with child processes. See discussion here: <https://github.com/apache/airflow/discussions/50170#discussioncomment-13265000>.
            // This will be considered as part of this issue: <https://github.com/stackabletech/airflow-operator/issues/641>.
            env.insert(
                "AIRFLOW__API__WORKERS".into(),
                EnvVar {
                    name: "AIRFLOW__API__WORKERS".into(),
                    value: Some("1".into()),
                    ..Default::default()
                },
            );
        }
    } else {
        env.insert(
            AIRFLOW_API_AUTH_BACKENDS.into(),
            EnvVar {
                name: AIRFLOW_API_AUTH_BACKENDS.into(),
                value: Some(
                    "airflow.api.auth.backend.basic_auth, airflow.api.auth.backend.session".into(),
                ),
                ..Default::default()
            },
        );
    }
}

// Internally the environment variable collection uses a map so that overrides can actually
// override existing keys. The returned collection will be a vector.
fn transform_map_to_vec(env_map: BTreeMap<String, EnvVar>) -> Vec<EnvVar> {
    env_map.into_values().collect::<Vec<EnvVar>>()
}

fn authentication_env_vars(
    auth_config: &AirflowClientAuthenticationDetailsResolved,
) -> Vec<EnvVar> {
    // Different OIDC authentication entries can reference the same
    // client secret. It must be ensured that the env variables are only
    // added once in such a case.

    let mut oidc_client_credentials_secrets = BTreeSet::new();

    for auth_class_resolved in &auth_config.authentication_classes_resolved {
        match auth_class_resolved {
            AirflowAuthenticationClassResolved::Ldap { .. } => {}
            AirflowAuthenticationClassResolved::Oidc { oidc, .. } => {
                oidc_client_credentials_secrets
                    .insert(oidc.client_credentials_secret_ref.to_owned());
            }
        }
    }

    oidc_client_credentials_secrets
        .iter()
        .cloned()
        .flat_map(oidc::v1alpha1::AuthenticationProvider::client_credentials_env_var_mounts)
        .collect()
}

fn authorization_env_vars(authorization_config: &AirflowAuthorizationResolved) -> Vec<EnvVar> {
    let mut env = vec![];

    if authorization_config.opa.is_some() {
        env.push(EnvVar {
            name: AIRFLOW_CORE_AUTH_MANAGER.into(),
            value: Some("opa_auth_manager.opa_fab_auth_manager.OpaFabAuthManager".to_string()),
            ..Default::default()
        });
    }

    env
}

fn execution_server_env_vars(airflow: &v1alpha1::AirflowCluster) -> BTreeMap<String, EnvVar> {
    let mut env: BTreeMap<String, EnvVar> = BTreeMap::new();

    if let Some(name) = airflow.metadata.name.as_ref() {
        // The execution API server URL can be any webserver (if there
        // are multiple ones). Parse the list of webservers in a deterministic
        // way by iterating over a BTree map rather than the HashMap.
        if airflow.spec.webservers.as_ref().is_some() {
            let webserver = format!("{name}-webserver", name = name,);
            tracing::debug!("Webserver set [{webserver}]");
            // These settings are new in 3.x and will have no affect with earlier versions.
            env.insert(
                "AIRFLOW__CORE__EXECUTION_API_SERVER_URL".into(),
                EnvVar {
                    name: "AIRFLOW__CORE__EXECUTION_API_SERVER_URL".into(),
                    value: Some(format!("http://{webserver}:8080/execution/")),
                    ..Default::default()
                },
            );
            env.insert(
                "AIRFLOW__CORE__BASE_URL".into(),
                EnvVar {
                    name: "AIRFLOW__CORE__BASE_URL".into(),
                    value: Some(format!("http://{webserver}:8080/")),
                    ..Default::default()
                },
            );
        }
    }

    env
}
