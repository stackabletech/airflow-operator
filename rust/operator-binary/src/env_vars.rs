use crate::util::env_var_from_secret;
use product_config::types::PropertyNameKind;
use stackable_airflow_crd::git_sync::GitSync;
use stackable_airflow_crd::{
    AirflowCluster, AirflowConfig, AirflowExecutor, AirflowRole, LOG_CONFIG_DIR,
};
use stackable_airflow_crd::{GIT_LINK, GIT_SYNC_DIR, TEMPLATE_LOCATION, TEMPLATE_NAME};
use stackable_operator::k8s_openapi::api::core::v1::EnvVar;
use stackable_operator::kube::ResourceExt;
use std::collections::{BTreeMap, HashMap};

const AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS: &str = "AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS";
const AIRFLOW__METRICS__STATSD_ON: &str = "AIRFLOW__METRICS__STATSD_ON";
const AIRFLOW__METRICS__STATSD_HOST: &str = "AIRFLOW__METRICS__STATSD_HOST";
const AIRFLOW__METRICS__STATSD_PORT: &str = "AIRFLOW__METRICS__STATSD_PORT";
const GITSYNC_USERNAME: &str = "GITSYNC_USERNAME";
const GITSYNC_PASSWORD: &str = "GITSYNC_PASSWORD";
const AIRFLOW__API__AUTH_BACKEND: &str = "AIRFLOW__API__AUTH_BACKEND";
const AIRFLOW__WEBSERVER__SECRET_KEY: &str = "AIRFLOW__WEBSERVER__SECRET_KEY";
const AIRFLOW__CORE__SQL_ALCHEMY_CONN: &str = "AIRFLOW__CORE__SQL_ALCHEMY_CONN";
const AIRFLOW__CELERY__RESULT_BACKEND: &str = "AIRFLOW__CELERY__RESULT_BACKEND";
const AIRFLOW__CELERY__BROKER_URL: &str = "AIRFLOW__CELERY__BROKER_URL";
const AIRFLOW__CORE__DAGS_FOLDER: &str = "AIRFLOW__CORE__DAGS_FOLDER";
const PYTHONPATH: &str = "PYTHONPATH";
const AIRFLOW__CORE__LOAD_EXAMPLES: &str = "AIRFLOW__CORE__LOAD_EXAMPLES";
const AIRFLOW__WEBSERVER__EXPOSE_CONFIG: &str = "AIRFLOW__WEBSERVER__EXPOSE_CONFIG";
const AIRFLOW__CORE__EXECUTOR: &str = "AIRFLOW__CORE__EXECUTOR";
const AIRFLOW__KUBERNETES_EXECUTOR__POD_TEMPLATE_FILE: &str =
    "AIRFLOW__KUBERNETES_EXECUTOR__POD_TEMPLATE_FILE";
const AIRFLOW__KUBERNETES_EXECUTOR__NAMESPACE: &str = "AIRFLOW__KUBERNETES_EXECUTOR__NAMESPACE";
const ADMIN_USERNAME: &str = "ADMIN_USERNAME";
const ADMIN_FIRSTNAME: &str = "ADMIN_FIRSTNAME";
const ADMIN_LASTNAME: &str = "ADMIN_LASTNAME";
const ADMIN_EMAIL: &str = "ADMIN_EMAIL";
const ADMIN_PASSWORD: &str = "ADMIN_PASSWORD";

pub fn build_airflow_statefulset_envs(
    airflow: &AirflowCluster,
    airflow_role: &AirflowRole,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    executor: &AirflowExecutor,
) -> Vec<EnvVar> {
    let mut env = vec![];

    env.extend(static_envs());

    let secret_prop = rolegroup_config
        .get(&PropertyNameKind::Env)
        .and_then(|vars| vars.get(AirflowConfig::CREDENTIALS_SECRET_PROPERTY));

    let secret_env = secret_prop
        .map(|secret| {
            vec![
                // The secret key is used to run the webserver flask app and also used to authorize
                // requests to Celery workers when logs are retrieved.
                env_var_from_secret(
                    AIRFLOW__WEBSERVER__SECRET_KEY,
                    secret,
                    "connections.secretKey",
                ),
                env_var_from_secret(
                    AIRFLOW__CORE__SQL_ALCHEMY_CONN,
                    secret,
                    "connections.sqlalchemyDatabaseUri",
                ),
                env_var_from_secret(
                    AIRFLOW__CELERY__RESULT_BACKEND,
                    secret,
                    "connections.celeryResultBackend",
                ),
                env_var_from_secret(
                    AIRFLOW__CELERY__BROKER_URL,
                    secret,
                    "connections.celeryBrokerUrl",
                ),
            ]
        })
        .unwrap_or_default();

    env.extend(secret_env);

    if let Some(GitSync {
        git_folder: Some(dags_folder),
        ..
    }) = airflow.git_sync()
    {
        env.push(EnvVar {
            name: AIRFLOW__CORE__DAGS_FOLDER.into(),
            value: Some(format!("{GIT_SYNC_DIR}/{GIT_LINK}/{dags_folder}")),
            ..Default::default()
        });
    } else {
        // if this has not been set for dag-provisioning visa gitsync (above), set the default value so that
        // PYTHONPATH can refer to this. See https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#dags-folder
        env.push(EnvVar {
            name: AIRFLOW__CORE__DAGS_FOLDER.into(),
            value: Some("$AIRFLOW_HOME/dags".to_string()),
            ..Default::default()
        });
    }

    if airflow.spec.cluster_config.load_examples {
        env.push(EnvVar {
            name: AIRFLOW__CORE__LOAD_EXAMPLES.into(),
            value: Some("True".into()),
            ..Default::default()
        })
    } else {
        env.push(EnvVar {
            name: AIRFLOW__CORE__LOAD_EXAMPLES.into(),
            value: Some("False".into()),
            ..Default::default()
        })
    }

    if airflow.spec.cluster_config.expose_config {
        env.push(EnvVar {
            name: AIRFLOW__WEBSERVER__EXPOSE_CONFIG.into(),
            value: Some("True".into()),
            ..Default::default()
        })
    }

    env.push(EnvVar {
        name: AIRFLOW__CORE__EXECUTOR.into(),
        value: Some(executor.to_string()),
        ..Default::default()
    });

    if let AirflowExecutor::KubernetesExecutor { .. } = executor {
        env.push(EnvVar {
            name: AIRFLOW__KUBERNETES_EXECUTOR__POD_TEMPLATE_FILE.into(),
            value: Some(format!("{TEMPLATE_LOCATION}/{TEMPLATE_NAME}")),
            ..Default::default()
        });
        env.push(EnvVar {
            name: AIRFLOW__KUBERNETES_EXECUTOR__NAMESPACE.into(),
            value: airflow.namespace(),
            ..Default::default()
        });
    }

    // Database initialization is limited to the scheduler, see https://github.com/stackabletech/airflow-operator/issues/259
    if airflow_role == &AirflowRole::Scheduler {
        let secret = &airflow.spec.cluster_config.credentials_secret;
        env.extend(vec![
            env_var_from_secret(ADMIN_USERNAME, secret, "adminUser.username"),
            env_var_from_secret(ADMIN_FIRSTNAME, secret, "adminUser.firstname"),
            env_var_from_secret(ADMIN_LASTNAME, secret, "adminUser.lastname"),
            env_var_from_secret(ADMIN_EMAIL, secret, "adminUser.email"),
            env_var_from_secret(ADMIN_PASSWORD, secret, "adminUser.password"),
        ]);
    }

    env
}

fn static_envs() -> Vec<EnvVar> {
    [
        EnvVar {
            // PYTHONPATH must be extended to include the dags folder so that dag
            // dependencies can be found.
            name: PYTHONPATH.into(),
            value: Some(format!("{LOG_CONFIG_DIR}:${AIRFLOW__CORE__DAGS_FOLDER}")),
            ..Default::default()
        },
        EnvVar {
            name: AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS.into(),
            value: Some("log_config.LOGGING_CONFIG".into()),
            ..Default::default()
        },
        EnvVar {
            name: AIRFLOW__METRICS__STATSD_ON.into(),
            value: Some("True".into()),
            ..Default::default()
        },
        EnvVar {
            name: AIRFLOW__METRICS__STATSD_HOST.into(),
            value: Some("0.0.0.0".into()),
            ..Default::default()
        },
        EnvVar {
            name: AIRFLOW__METRICS__STATSD_PORT.into(),
            value: Some("9125".into()),
            ..Default::default()
        },
        // Authentication for the API is handled separately to the Web Authentication.
        // Basic authentication is used by the integration tests.
        // The default is to deny all requests to the API.
        EnvVar {
            name: AIRFLOW__API__AUTH_BACKEND.into(),
            value: Some("airflow.api.auth.backend.basic_auth".into()),
            ..Default::default()
        },
    ]
    .into()
}

pub fn build_gitsync_statefulset_envs(
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Vec<EnvVar> {
    let mut env = vec![];

    if let Some(git_secret) = rolegroup_config
        .get(&PropertyNameKind::Env)
        .and_then(|vars| vars.get(AirflowConfig::GIT_CREDENTIALS_SECRET_PROPERTY))
    {
        env.push(env_var_from_secret(GITSYNC_USERNAME, git_secret, "user"));
        env.push(env_var_from_secret(
            GITSYNC_PASSWORD,
            git_secret,
            "password",
        ));
    }

    env
}

pub fn build_airflow_template_envs(
    airflow: &AirflowCluster,
    env_overrides: &HashMap<String, String>,
) -> Vec<EnvVar> {
    let secret_prop = Some(airflow.spec.cluster_config.credentials_secret.as_str());

    let mut env = secret_prop
        .map(|secret| {
            vec![env_var_from_secret(
                AIRFLOW__CORE__SQL_ALCHEMY_CONN,
                secret,
                "connections.sqlalchemyDatabaseUri",
            )]
        })
        .unwrap_or_default();

    env.push(EnvVar {
        name: AIRFLOW__CORE__EXECUTOR.into(),
        value: Some("LocalExecutor".to_string()),
        ..Default::default()
    });
    env.push(EnvVar {
        name: AIRFLOW__KUBERNETES_EXECUTOR__NAMESPACE.into(),
        value: airflow.namespace(),
        ..Default::default()
    });

    env.extend(static_envs());

    // iterate over a BTreeMap to ensure the vars are written in a predictable order
    for (k, v) in env_overrides.iter().collect::<BTreeMap<_, _>>() {
        env.push(EnvVar {
            name: k.to_string(),
            value: Some(v.to_string()),
            ..Default::default()
        });
    }

    env
}

pub fn build_gitsync_template(credentials_secret: &Option<String>) -> Vec<EnvVar> {
    let mut env = vec![];

    if let Some(credentials_secret) = &credentials_secret {
        env.push(env_var_from_secret(
            GITSYNC_USERNAME,
            credentials_secret,
            "user",
        ));
        env.push(env_var_from_secret(
            GITSYNC_PASSWORD,
            credentials_secret,
            "password",
        ));
    }
    env
}
