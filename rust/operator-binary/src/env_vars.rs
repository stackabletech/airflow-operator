use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::PathBuf,
};

use product_config::types::PropertyNameKind;
use snafu::{OptionExt, Snafu};
use stackable_operator::{
    commons::authentication::oidc, k8s_openapi::api::core::v1::EnvVar, kube::ResourceExt,
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
        git_sync::{GIT_SYNC_DIR, GIT_SYNC_LINK, GitSync},
        v1alpha1,
    },
    util::env_var_from_secret,
};

const AIRFLOW_CORE_AUTH_MANAGER: &str = "AIRFLOW__CORE__AUTH_MANAGER";
const AIRFLOW_LOGGING_LOGGING_CONFIG_CLASS: &str = "AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS";
const AIRFLOW_METRICS_STATSD_ON: &str = "AIRFLOW__METRICS__STATSD_ON";
const AIRFLOW_METRICS_STATSD_HOST: &str = "AIRFLOW__METRICS__STATSD_HOST";
const AIRFLOW_METRICS_STATSD_PORT: &str = "AIRFLOW__METRICS__STATSD_PORT";
const AIRFLOW_API_AUTH_BACKEND: &str = "AIRFLOW__API__AUTH_BACKEND";
const AIRFLOW_WEBSERVER_SECRET_KEY: &str = "AIRFLOW__WEBSERVER__SECRET_KEY";
const AIRFLOW_CORE_SQL_ALCHEMY_CONN: &str = "AIRFLOW__CORE__SQL_ALCHEMY_CONN";
const AIRFLOW_CELERY_RESULT_BACKEND: &str = "AIRFLOW__CELERY__RESULT_BACKEND";
const AIRFLOW_CELERY_BROKER_URL: &str = "AIRFLOW__CELERY__BROKER_URL";
const AIRFLOW_CORE_DAGS_FOLDER: &str = "AIRFLOW__CORE__DAGS_FOLDER";
const AIRFLOW_CORE_LOAD_EXAMPLES: &str = "AIRFLOW__CORE__LOAD_EXAMPLES";
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

const GITSYNC_USERNAME: &str = "GITSYNC_USERNAME";
const GITSYNC_PASSWORD: &str = "GITSYNC_PASSWORD";

const PYTHONPATH: &str = "PYTHONPATH";

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
pub fn build_airflow_statefulset_envs(
    airflow: &v1alpha1::AirflowCluster,
    airflow_role: &AirflowRole,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    executor: &AirflowExecutor,
    auth_config: &AirflowClientAuthenticationDetailsResolved,
    authorization_config: &AirflowAuthorizationResolved,
) -> Result<Vec<EnvVar>, Error> {
    let mut env: BTreeMap<String, EnvVar> = BTreeMap::new();

    env.extend(static_envs(airflow)?);

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
            AIRFLOW_CORE_SQL_ALCHEMY_CONN.into(),
            env_var_from_secret(
                AIRFLOW_CORE_SQL_ALCHEMY_CONN,
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

    let dags_folder = get_dags_folder(airflow)?;
    env.insert(AIRFLOW_CORE_DAGS_FOLDER.into(), EnvVar {
        name: AIRFLOW_CORE_DAGS_FOLDER.into(),
        value: Some(dags_folder),
        ..Default::default()
    });

    if airflow.spec.cluster_config.load_examples {
        env.insert(AIRFLOW_CORE_LOAD_EXAMPLES.into(), EnvVar {
            name: AIRFLOW_CORE_LOAD_EXAMPLES.into(),
            value: Some("True".into()),
            ..Default::default()
        });
    } else {
        env.insert(AIRFLOW_CORE_LOAD_EXAMPLES.into(), EnvVar {
            name: AIRFLOW_CORE_LOAD_EXAMPLES.into(),
            value: Some("False".into()),
            ..Default::default()
        });
    }

    if airflow.spec.cluster_config.expose_config {
        env.insert(AIRFLOW_WEBSERVER_EXPOSE_CONFIG.into(), EnvVar {
            name: AIRFLOW_WEBSERVER_EXPOSE_CONFIG.into(),
            value: Some("True".into()),
            ..Default::default()
        });
    }

    env.insert(AIRFLOW_CORE_EXECUTOR.into(), EnvVar {
        name: AIRFLOW_CORE_EXECUTOR.into(),
        value: Some(executor.to_string()),
        ..Default::default()
    });

    if let AirflowExecutor::KubernetesExecutor { .. } = executor {
        env.insert(
            AIRFLOW_KUBERNETES_EXECUTOR_POD_TEMPLATE_FILE.into(),
            EnvVar {
                name: AIRFLOW_KUBERNETES_EXECUTOR_POD_TEMPLATE_FILE.into(),
                value: Some(format!("{TEMPLATE_LOCATION}/{TEMPLATE_NAME}")),
                ..Default::default()
            },
        );
        env.insert(AIRFLOW_KUBERNETES_EXECUTOR_NAMESPACE.into(), EnvVar {
            name: AIRFLOW_KUBERNETES_EXECUTOR_NAMESPACE.into(),
            value: airflow.namespace(),
            ..Default::default()
        });
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
            env.insert(k.into(), EnvVar {
                name: k.to_string(),
                value: Some(v.to_string()),
                ..Default::default()
            });
        }
    }

    // Needed for the `containerdebug` process to log it's tracing information to.
    env.insert("CONTAINERDEBUG_LOG_DIRECTORY".to_string(), EnvVar {
        name: "CONTAINERDEBUG_LOG_DIRECTORY".to_string(),
        value: Some(format!("{STACKABLE_LOG_DIR}/containerdebug")),
        value_from: None,
    });

    tracing::debug!("Env-var set [{:?}]", env);
    Ok(transform_map_to_vec(env))
}

pub fn get_dags_folder(airflow: &v1alpha1::AirflowCluster) -> Result<String, Error> {
    match airflow.git_sync() {
        Some(GitSync { git_folder, .. }) => {
            let mut dag_folder = PathBuf::from(GIT_SYNC_DIR);
            dag_folder.push(GIT_SYNC_LINK);
            // Remove trailing slash
            let sanitized_git_folder = git_folder.strip_prefix("/").unwrap_or(git_folder);
            dag_folder.push(sanitized_git_folder);
            Ok(dag_folder
                .to_str()
                .with_context(|| ConstructGitDagFolderSnafu {
                    dag_folder: dag_folder.clone(),
                })?
                .to_string())
        }
        None => {
            // if this has not been set for dag-provisioning via gitsync (above), set the default value
            // so that PYTHONPATH can refer to this. N.B. nested variables need to be resolved, so that
            // /stackable/airflow is used instead of $AIRFLOW_HOME.
            // See https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#dags-folder
            Ok("/stackable/airflow/dags".to_string())
        }
    }
}

// This set of environment variables is a standard set that is not dependent on any
// conditional logic and should be applied to the statefulset or the executor template config map.
fn static_envs(airflow: &v1alpha1::AirflowCluster) -> Result<BTreeMap<String, EnvVar>, Error> {
    let mut env: BTreeMap<String, EnvVar> = BTreeMap::new();

    let dags_folder = get_dags_folder(airflow)?;

    env.insert(PYTHONPATH.into(), EnvVar {
        // PYTHONPATH must be extended to include the dags folder so that dag
        // dependencies can be found: this must be the actual path and not a variable.
        // Also include the airflow site-packages by default (for airflow and kubernetes classes etc.)
        name: PYTHONPATH.into(),
        value: Some(format!("{LOG_CONFIG_DIR}:{dags_folder}")),
        ..Default::default()
    });
    env.insert(AIRFLOW_LOGGING_LOGGING_CONFIG_CLASS.into(), EnvVar {
        name: AIRFLOW_LOGGING_LOGGING_CONFIG_CLASS.into(),
        value: Some("log_config.LOGGING_CONFIG".into()),
        ..Default::default()
    });

    env.insert(AIRFLOW_METRICS_STATSD_ON.into(), EnvVar {
        name: AIRFLOW_METRICS_STATSD_ON.into(),
        value: Some("True".into()),
        ..Default::default()
    });

    env.insert(AIRFLOW_METRICS_STATSD_HOST.into(), EnvVar {
        name: AIRFLOW_METRICS_STATSD_HOST.into(),
        value: Some("0.0.0.0".into()),
        ..Default::default()
    });

    env.insert(AIRFLOW_METRICS_STATSD_PORT.into(), EnvVar {
        name: AIRFLOW_METRICS_STATSD_PORT.into(),
        value: Some("9125".into()),
        ..Default::default()
    });

    env.insert(
        AIRFLOW_API_AUTH_BACKEND.into(),
        // Authentication for the API is handled separately to the Web Authentication.
        // Basic authentication is used by the integration tests.
        // The default is to deny all requests to the API.
        EnvVar {
            name: AIRFLOW_API_AUTH_BACKEND.into(),
            value: Some("airflow.api.auth.backend.basic_auth".into()),
            ..Default::default()
        },
    );
    Ok(env)
}

/// Return environment variables to be applied to the gitsync container in the statefulset for the scheduler,
/// webserver (and worker, for clusters utilizing `celeryExecutor`). N.B. the git credentials-secret is passed
/// explicitly here: it is no longer added to the role config (lib/compute_env) as the kubenertes executor wraps
/// `CommonConfiguration<ExecutorConfigFragment>` that is not linked to a role.
pub fn build_gitsync_statefulset_envs(
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    git_credentials_secret: &Option<String>,
) -> Vec<EnvVar> {
    let mut env: BTreeMap<String, EnvVar> = BTreeMap::new();

    add_gitsync_credentials(git_credentials_secret, &mut env);

    if let Some(git_env) = rolegroup_config.get(&PropertyNameKind::Env) {
        for (k, v) in git_env.iter() {
            gitsync_vars_map(k, &mut env, v);
        }
    }

    tracing::debug!("Env-var set [{:?}]", env);
    transform_map_to_vec(env)
}

fn add_gitsync_credentials(
    git_credentials_secret: &Option<String>,
    env: &mut BTreeMap<String, EnvVar>,
) {
    if let Some(git_credentials_secret) = &git_credentials_secret {
        env.insert(
            GITSYNC_USERNAME.to_string(),
            env_var_from_secret(GITSYNC_USERNAME, git_credentials_secret, "user"),
        );
        env.insert(
            GITSYNC_PASSWORD.to_string(),
            env_var_from_secret(GITSYNC_PASSWORD, git_credentials_secret, "password"),
        );
    }
}

/// Return environment variables to be applied to the configuration map used in conjunction with
/// the `kubernetesExecutor` worker.
pub fn build_airflow_template_envs(
    airflow: &v1alpha1::AirflowCluster,
    env_overrides: &HashMap<String, String>,
    config: &ExecutorConfig,
) -> Result<Vec<EnvVar>, Error> {
    let mut env: BTreeMap<String, EnvVar> = BTreeMap::new();
    let secret = airflow.spec.cluster_config.credentials_secret.as_str();

    env.insert(
        AIRFLOW_CORE_SQL_ALCHEMY_CONN.into(),
        env_var_from_secret(
            AIRFLOW_CORE_SQL_ALCHEMY_CONN,
            secret,
            "connections.sqlalchemyDatabaseUri",
        ),
    );

    env.insert(AIRFLOW_CORE_EXECUTOR.into(), EnvVar {
        name: AIRFLOW_CORE_EXECUTOR.into(),
        value: Some("LocalExecutor".to_string()),
        ..Default::default()
    });

    env.insert(AIRFLOW_KUBERNETES_EXECUTOR_NAMESPACE.into(), EnvVar {
        name: AIRFLOW_KUBERNETES_EXECUTOR_NAMESPACE.into(),
        value: airflow.namespace(),
        ..Default::default()
    });

    // the config map also requires the dag-folder location as this will be passed on
    // to the pods started by airflow.
    let dags_folder = get_dags_folder(airflow)?;
    env.insert(AIRFLOW_CORE_DAGS_FOLDER.into(), EnvVar {
        name: AIRFLOW_CORE_DAGS_FOLDER.into(),
        value: Some(dags_folder),
        ..Default::default()
    });

    env.extend(static_envs(airflow)?);

    // _STACKABLE_POST_HOOK will contain a command to create a shutdown hook that will be
    // evaluated in the wrapper for each stackable spark container: this is necessary for pods
    // that are created and then terminated (we do a similar thing for spark-k8s).
    if config.logging.enable_vector_agent {
        env.insert("_STACKABLE_POST_HOOK".into(), EnvVar {
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
        });
    }

    // iterate over a BTreeMap to ensure the vars are written in a predictable order
    for (k, v) in env_overrides.iter().collect::<BTreeMap<_, _>>() {
        env.insert(k.to_string(), EnvVar {
            name: k.to_string(),
            value: Some(v.to_string()),
            ..Default::default()
        });
    }

    tracing::debug!("Env-var set [{:?}]", env);
    Ok(transform_map_to_vec(env))
}

/// Return environment variables to be applied to the configuration map used in conjunction with
/// the `kubernetesExecutor` worker: applied to the gitsync `initContainer`.
pub fn build_gitsync_template(
    env_overrides: &HashMap<String, String>,
    git_credentials_secret: &Option<String>,
) -> Vec<EnvVar> {
    let mut env: BTreeMap<String, EnvVar> = BTreeMap::new();

    add_gitsync_credentials(git_credentials_secret, &mut env);

    for (k, v) in env_overrides.iter().collect::<BTreeMap<_, _>>() {
        gitsync_vars_map(k, &mut env, v);
    }

    tracing::debug!("Env-var set [{:?}]", env);
    transform_map_to_vec(env)
}

fn gitsync_vars_map(k: &String, env: &mut BTreeMap<String, EnvVar>, v: &String) {
    env.insert(k.to_string(), EnvVar {
        name: k.to_string(),
        value: Some(v.to_string()),
        ..Default::default()
    });
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
        .flat_map(oidc::AuthenticationProvider::client_credentials_env_var_mounts)
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
