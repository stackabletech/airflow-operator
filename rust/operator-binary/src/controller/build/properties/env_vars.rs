use std::{collections::BTreeSet, path::PathBuf, str::FromStr};

use snafu::Snafu;
use stackable_operator::{
    constant,
    crd::{authentication::oidc, git_sync},
    kube::ResourceExt,
    product_logging::framework::create_vector_shutdown_file_command,
    v2::{
        builder::pod::container::{EnvVarName, EnvVarSet},
        product_logging::framework::STACKABLE_LOG_DIR,
        types::kubernetes::SecretKey,
    },
};

use crate::{
    controller::{ValidatedCluster, ValidatedLogging},
    crd::{
        AirflowExecutor, AirflowRole, HTTP_PORT, LOG_CONFIG_DIR, TEMPLATE_LOCATION, TEMPLATE_NAME,
        authentication::{
            AirflowAuthenticationClassResolved, AirflowClientAuthenticationDetailsResolved,
        },
        authorization::AirflowAuthorizationResolved,
        internal_secret::{
            FERNET_KEY_SECRET_KEY, INTERNAL_SECRET_SECRET_KEY, JWT_SECRET_SECRET_KEY,
        },
        trusted_proxies::TrustedProxy,
    },
    util::role_service_name,
};

constant!(AIRFLOW_CORE_AUTH_MANAGER: EnvVarName = "AIRFLOW__CORE__AUTH_MANAGER");
// Airflow 3 envs
constant!(AIRFLOW_CORE_AUTH_OPA_REQUEST_URL: EnvVarName = "AIRFLOW__CORE__AUTH_OPA_REQUEST_URL");
constant!(AIRFLOW_CORE_AUTH_OPA_CACHE_TTL_IN_SEC: EnvVarName = "AIRFLOW__CORE__AUTH_OPA_CACHE_TTL_IN_SEC");
constant!(AIRFLOW_CORE_AUTH_OPA_CACHE_MAXSIZE: EnvVarName = "AIRFLOW__CORE__AUTH_OPA_CACHE_MAXSIZE");

constant!(AIRFLOW_LOGGING_LOGGING_CONFIG_CLASS: EnvVarName = "AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS");
constant!(AIRFLOW_METRICS_STATSD_ON: EnvVarName = "AIRFLOW__METRICS__STATSD_ON");
constant!(AIRFLOW_METRICS_STATSD_HOST: EnvVarName = "AIRFLOW__METRICS__STATSD_HOST");
constant!(AIRFLOW_METRICS_STATSD_PORT: EnvVarName = "AIRFLOW__METRICS__STATSD_PORT");
constant!(AIRFLOW_WEBSERVER_SECRET_KEY: EnvVarName = "AIRFLOW__WEBSERVER__SECRET_KEY");
constant!(AIRFLOW_API_SECRET_KEY: EnvVarName = "AIRFLOW__API__SECRET_KEY");
constant!(AIRFLOW_CORE_FERNET_KEY: EnvVarName = "AIRFLOW__CORE__FERNET_KEY");
constant!(AIRFLOW_CELERY_RESULT_BACKEND: EnvVarName = "AIRFLOW__CELERY__RESULT_BACKEND");
constant!(AIRFLOW_CELERY_BROKER_URL: EnvVarName = "AIRFLOW__CELERY__BROKER_URL");
constant!(AIRFLOW_CORE_DAGS_FOLDER: EnvVarName = "AIRFLOW__CORE__DAGS_FOLDER");
constant!(AIRFLOW_CORE_LOAD_EXAMPLES: EnvVarName = "AIRFLOW__CORE__LOAD_EXAMPLES");
constant!(AIRFLOW_API_AUTH_BACKENDS: EnvVarName = "AIRFLOW__API__AUTH_BACKENDS");
constant!(AIRFLOW_WEBSERVER_ENABLE_PROXY_FIX: EnvVarName = "AIRFLOW__WEBSERVER__ENABLE_PROXY_FIX");
constant!(AIRFLOW_WEBSERVER_PROXY_FIX_X_FOR: EnvVarName = "AIRFLOW__WEBSERVER__PROXY_FIX_X_FOR");
constant!(AIRFLOW_SCHEDULER_STANDALONE_DAG_PROCESSOR: EnvVarName = "AIRFLOW__SCHEDULER__STANDALONE_DAG_PROCESSOR");
constant!(AIRFLOW_API_AUTH_JWT_SECRET: EnvVarName = "AIRFLOW__API_AUTH__JWT_SECRET");
constant!(AIRFLOW_API_WORKERS: EnvVarName = "AIRFLOW__API__WORKERS");
constant!(AIRFLOW_FAB_UPDATE_FAB_PERMS: EnvVarName = "AIRFLOW__FAB__UPDATE_FAB_PERMS");
constant!(FORWARDED_ALLOW_IPS: EnvVarName = "FORWARDED_ALLOW_IPS");
constant!(AIRFLOW_DATABASE_SQL_ALCHEMY_CONN: EnvVarName = "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN");

constant!(AIRFLOW_WEBSERVER_EXPOSE_CONFIG: EnvVarName = "AIRFLOW__WEBSERVER__EXPOSE_CONFIG");
constant!(AIRFLOW_CORE_EXECUTOR: EnvVarName = "AIRFLOW__CORE__EXECUTOR");
constant!(AIRFLOW_KUBERNETES_EXECUTOR_POD_TEMPLATE_FILE: EnvVarName =
    "AIRFLOW__KUBERNETES_EXECUTOR__POD_TEMPLATE_FILE");
constant!(AIRFLOW_KUBERNETES_EXECUTOR_NAMESPACE: EnvVarName = "AIRFLOW__KUBERNETES_EXECUTOR__NAMESPACE");

constant!(AIRFLOW_CORE_EXECUTION_API_SERVER_URL: EnvVarName = "AIRFLOW__CORE__EXECUTION_API_SERVER_URL");
constant!(AIRFLOW_CORE_BASE_URL: EnvVarName = "AIRFLOW__CORE__BASE_URL");

constant!(ADMIN_FIRSTNAME: EnvVarName = "ADMIN_FIRSTNAME");
constant!(ADMIN_USERNAME: EnvVarName = "ADMIN_USERNAME");
constant!(ADMIN_LASTNAME: EnvVarName = "ADMIN_LASTNAME");
constant!(ADMIN_PASSWORD: EnvVarName = "ADMIN_PASSWORD");
constant!(ADMIN_EMAIL: EnvVarName = "ADMIN_EMAIL");

constant!(ADMIN_USERNAME_SECRET_KEY: SecretKey = "adminUser.username");
constant!(ADMIN_FIRSTNAME_SECRET_KEY: SecretKey = "adminUser.firstname");
constant!(ADMIN_LASTNAME_SECRET_KEY: SecretKey = "adminUser.lastname");
constant!(ADMIN_EMAIL_SECRET_KEY: SecretKey = "adminUser.email");
constant!(ADMIN_PASSWORD_SECRET_KEY: SecretKey = "adminUser.password");

constant!(PYTHONPATH: EnvVarName = "PYTHONPATH");

constant!(CONTAINERDEBUG_LOG_DIRECTORY: EnvVarName = "CONTAINERDEBUG_LOG_DIRECTORY");
constant!(STACKABLE_POST_HOOK: EnvVarName = "_STACKABLE_POST_HOOK");

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
    cluster: &ValidatedCluster,
    airflow_role: &AirflowRole,
    env_overrides: &EnvVarSet,
    git_sync_resources: &git_sync::v1alpha2::GitSyncResources,
) -> EnvVarSet {
    let executor = &cluster.cluster_config.executor;
    let auth_config = &cluster.cluster_config.authentication_config;
    let authorization_config = &cluster.cluster_config.authorization_config;
    let resolved_product_image = &cluster.image;
    let metadata_database_connection_details = cluster.metadata_database_connection_details();
    let celery_database_connection_details = cluster.celery_database_connection_details();

    let internal_secret_name = cluster.internal_secret_name();

    let mut env_vars = static_envs(git_sync_resources)
        .merge(add_version_specific_env_vars(cluster, airflow_role))
        // N.B. this has been deprecated and replaced with AIRFLOW__API__SECRET_KEY since 3.0.2. Can be removed when 3.0.1 is no longer supported.
        .with_secret_key_ref(
            &AIRFLOW_WEBSERVER_SECRET_KEY,
            &internal_secret_name,
            &INTERNAL_SECRET_SECRET_KEY,
        )
        // Replaces AIRFLOW__WEBSERVER__SECRET_KEY >= 3.0.2.
        .with_secret_key_ref(
            &AIRFLOW_API_SECRET_KEY,
            &internal_secret_name,
            &INTERNAL_SECRET_SECRET_KEY,
        )
        .with_secret_key_ref(
            &AIRFLOW_CORE_FERNET_KEY,
            &cluster.fernet_key_name(),
            &FERNET_KEY_SECRET_KEY,
        )
        .with_value(
            &AIRFLOW_DATABASE_SQL_ALCHEMY_CONN,
            metadata_database_connection_details.url_template,
        );

    // Only needed when celery executors are used
    if let Some((celery_result_backend, celery_broker)) = celery_database_connection_details {
        env_vars = env_vars
            .with_value(
                &AIRFLOW_CELERY_RESULT_BACKEND,
                celery_result_backend.url_template,
            )
            .with_value(&AIRFLOW_CELERY_BROKER_URL, celery_broker.url_template);
    }

    let dags_folder = get_dags_folder(git_sync_resources);
    env_vars = env_vars
        .with_value(&AIRFLOW_CORE_DAGS_FOLDER, dags_folder)
        .with_value(
            &AIRFLOW_CORE_LOAD_EXAMPLES,
            if cluster.cluster_config.load_examples {
                "True"
            } else {
                "False"
            },
        );

    if cluster.cluster_config.expose_config {
        env_vars = env_vars.with_value(&AIRFLOW_WEBSERVER_EXPOSE_CONFIG, "True");
    }

    env_vars = env_vars.with_value(&AIRFLOW_CORE_EXECUTOR, executor.as_airflow_core_executor());

    if let AirflowExecutor::KubernetesExecutors { .. } = executor {
        env_vars = env_vars
            .with_value(
                &AIRFLOW_KUBERNETES_EXECUTOR_POD_TEMPLATE_FILE,
                format!("{TEMPLATE_LOCATION}/{TEMPLATE_NAME}"),
            )
            .with_value(&AIRFLOW_KUBERNETES_EXECUTOR_NAMESPACE, &cluster.namespace);
    }

    match airflow_role {
        // Database initialization is limited to the scheduler.
        // See https://github.com/stackabletech/airflow-operator/issues/259
        AirflowRole::Scheduler => {
            let secret = &cluster.cluster_config.credentials_secret_name;
            env_vars = env_vars
                .with_secret_key_ref(&ADMIN_USERNAME, secret, &ADMIN_USERNAME_SECRET_KEY)
                .with_secret_key_ref(&ADMIN_FIRSTNAME, secret, &ADMIN_FIRSTNAME_SECRET_KEY)
                .with_secret_key_ref(&ADMIN_LASTNAME, secret, &ADMIN_LASTNAME_SECRET_KEY)
                .with_secret_key_ref(&ADMIN_EMAIL, secret, &ADMIN_EMAIL_SECRET_KEY)
                .with_secret_key_ref(&ADMIN_PASSWORD, secret, &ADMIN_PASSWORD_SECRET_KEY);
        }
        AirflowRole::Webserver => {
            env_vars =
                env_vars
                    .merge(authentication_env_vars(auth_config))
                    .merge(authorization_env_vars(
                        authorization_config,
                        &resolved_product_image.product_version,
                    ));
        }
        _ => {}
    }

    // Needed for the `containerdebug` process to log it's tracing information to.
    env_vars = env_vars
        .with_value(
            &CONTAINERDEBUG_LOG_DIRECTORY,
            format!("{STACKABLE_LOG_DIR}/containerdebug"),
        )
        // apply overrides last of all; `EnvVarSet` is keyed by name, so iteration is already
        // in a fixed (sorted-by-name) order
        .merge(env_overrides.clone());

    tracing::debug!("Env-var set: {:?}", Vec::from(&env_vars));

    env_vars
}

pub fn get_dags_folder(git_sync_resources: &git_sync::v1alpha2::GitSyncResources) -> String {
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
fn static_envs(git_sync_resources: &git_sync::v1alpha2::GitSyncResources) -> EnvVarSet {
    let dags_folder = get_dags_folder(git_sync_resources);

    EnvVarSet::new()
        // PYTHONPATH must be extended to include the dags folder so that dag
        // dependencies can be found: this must be the actual path and not a variable.
        // Also include the airflow site-packages by default (for airflow and kubernetes classes etc.)
        .with_value(&PYTHONPATH, format!("{LOG_CONFIG_DIR}:{dags_folder}"))
        .with_value(
            &AIRFLOW_LOGGING_LOGGING_CONFIG_CLASS,
            "log_config.LOGGING_CONFIG",
        )
        .with_value(&AIRFLOW_METRICS_STATSD_ON, "True")
        .with_value(&AIRFLOW_METRICS_STATSD_HOST, "0.0.0.0")
        .with_value(&AIRFLOW_METRICS_STATSD_PORT, "9125")
}

/// Return environment variables to be applied to the configuration map used in conjunction with
/// the `kubernetesExecutor` worker.
pub fn build_airflow_template_envs(
    cluster: &ValidatedCluster,
    env_overrides: &EnvVarSet,
    logging: &ValidatedLogging,
    git_sync_resources: &git_sync::v1alpha2::GitSyncResources,
) -> EnvVarSet {
    // the config map also requires the dag-folder location as this will be passed on
    // to the pods started by airflow.
    let dags_folder = get_dags_folder(git_sync_resources);

    let mut env_vars = EnvVarSet::new()
        .with_value(
            &AIRFLOW_DATABASE_SQL_ALCHEMY_CONN,
            cluster.metadata_database_connection_details().url_template,
        )
        .with_value(&AIRFLOW_CORE_EXECUTOR, "LocalExecutor")
        .with_value(&AIRFLOW_KUBERNETES_EXECUTOR_NAMESPACE, &cluster.namespace)
        .with_value(&AIRFLOW_CORE_DAGS_FOLDER, dags_folder)
        .merge(static_envs(git_sync_resources))
        .merge(add_version_specific_env_vars(cluster, &AirflowRole::Worker));

    // _STACKABLE_POST_HOOK will contain a command to create a shutdown hook that will be
    // evaluated in the wrapper for each stackable spark container: this is necessary for pods
    // that are created and then terminated (we do a similar thing for spark-k8s).
    if logging.enable_vector_agent {
        env_vars = env_vars.with_value(
            &STACKABLE_POST_HOOK,
            [
                // Wait for Vector to gather the logs.
                "sleep 10",
                &create_vector_shutdown_file_command(STACKABLE_LOG_DIR),
            ]
            .join("; "),
        );
    }

    env_vars = env_vars.merge(env_overrides.clone());

    tracing::debug!("Env-var set [{:?}]", env_vars);

    env_vars
}

fn add_version_specific_env_vars(
    cluster: &ValidatedCluster,
    airflow_role: &AirflowRole,
) -> EnvVarSet {
    let mut env_vars = EnvVarSet::new();

    if cluster.image.product_version.starts_with("3.") {
        env_vars = env_vars
            .merge(execution_server_env_vars(cluster))
            .with_value(
                &AIRFLOW_CORE_AUTH_MANAGER,
                "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager",
            )
            .with_value(
                &AIRFLOW_API_AUTH_BACKENDS,
                "airflow.api.auth.backend.session",
            )
            // As of 3.x a JWT key is required.
            // See https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html#jwt-secret
            // This should be random, but must also be consistent across
            // api-services and replicas/roles for a given
            // cluster, but should also be cluster-specific.
            // It is accessed from a secret to avoid cluster restarts
            // being triggered by an operator restart.
            .with_secret_key_ref(
                &AIRFLOW_API_AUTH_JWT_SECRET,
                &cluster.jwt_secret_name(),
                &JWT_SECRET_SECRET_KEY,
            )
            // The Airflow default for this is 4.
            // However, with the default resources this could cause problems,
            // as the Pod went to 100% CPU usage and didn't get healthy
            // quick enough, resulting in a crashloop.
            .with_value(&AIRFLOW_API_WORKERS, "1");
        if airflow_role == &AirflowRole::Webserver {
            // Sometimes a race condition can arise when both scheduler and
            // api-server are updating the DB, which adds overhead (conflicts
            // are logged) and can result in inconsistencies. This setting
            // ensure that only the scheduler will do this by default.
            env_vars = env_vars.with_value(&AIRFLOW_FAB_UPDATE_FAB_PERMS, "False");

            // This env var is needed in addition to `--proxy-headers` when Airflow runs
            // behind a reverse proxy.
            // This covers the uvicorn backend, the only one the SDP image can run.
            let trusted_proxies = cluster
                .role_configs
                .get(airflow_role)
                .map(|role_config| role_config.trusted_proxies.as_slice())
                .unwrap_or_default()
                .iter()
                .map(TrustedProxy::to_string)
                .collect::<Vec<_>>()
                .join(",");

            if !trusted_proxies.is_empty() {
                env_vars = env_vars.with_value(&FORWARDED_ALLOW_IPS, trusted_proxies);
            }
        }
    } else {
        env_vars = env_vars.with_value(
            &AIRFLOW_API_AUTH_BACKENDS,
            "airflow.api.auth.backend.basic_auth, airflow.api.auth.backend.session",
        );

        // The 2.x uses Werkzeug's `ProxyFix` to allow forwarded-headers and it does so regardless
        // of the peer source. The only valid value for `spec.webservers.roleConfig.trustedProxies` is `["*"]`.
        if airflow_role == &AirflowRole::Webserver {
            let trusted_proxies = cluster
                .role_configs
                .get(airflow_role)
                .map(|role_config| role_config.trusted_proxies.as_slice())
                .unwrap_or_default();

            if !trusted_proxies.is_empty() {
                env_vars = env_vars
                    .with_value(&AIRFLOW_WEBSERVER_ENABLE_PROXY_FIX, "True")
                    .with_value(&AIRFLOW_WEBSERVER_PROXY_FIX_X_FOR, "1");

                if !trusted_proxies.iter().any(TrustedProxy::is_wildcard) {
                    let product_version = &cluster.image.product_version;
                    tracing::warn!(
                        "spec.webservers.roleConfig.trustedProxies lists specific addresses, but \
                         Airflow {product_version}'s webserver has no way to restrict forwarded \
                         headers to specific peers -- once enabled it trusts X-Forwarded-* from \
                         any peer, the same as \"*\"",
                    );
                }
            }
        }

        if cluster.has_role(&AirflowRole::DagProcessor) {
            // In airflow 2.x the dag-processor can optionally be started as a
            // standalone process (rather then as a scheduler subprocess),
            // accompanied by this env-var being set to True.
            env_vars = env_vars.with_value(&AIRFLOW_SCHEDULER_STANDALONE_DAG_PROCESSOR, "True");
        }
    }

    env_vars
}

fn authentication_env_vars(auth_config: &AirflowClientAuthenticationDetailsResolved) -> EnvVarSet {
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
        .fold(EnvVarSet::new(),
            |env_vars, env_var| env_vars
                .with_env_var(env_var)
                .expect("env_var name is valid because it is either OIDC_<16-hex-characters>_CLIENT_ID or OIDC_<16-hex-characters>_CLIENT_SECRET")
        )
}

/// Constructs the needed authorization env vars for the specific Airflow version.
///
/// `AIRFLOW__CORE__AUTH_MANAGER` always needs to be set as env var.
///
/// Airflow 2 needs to OPA settings in the `webserver_config.py` such as `AUTH_OPA_REQUEST_URL`.
/// Airflow 3 needs to OPA settings as env variables such as `AIRFLOW__CORE__AUTH_OPA_REQUEST_URL`.
fn authorization_env_vars(
    authorization_config: &AirflowAuthorizationResolved,
    product_version: &str,
) -> EnvVarSet {
    let Some(opa) = &authorization_config.opa else {
        return EnvVarSet::new();
    };

    let mut env_vars = EnvVarSet::new().with_value(
        &AIRFLOW_CORE_AUTH_MANAGER,
        "opa_auth_manager.opa_fab_auth_manager.OpaFabAuthManager",
    );
    if product_version.starts_with("2.") {
        // OPA config needs to go into `webserver_config.py`
    } else {
        env_vars = env_vars
            .with_value(&AIRFLOW_CORE_AUTH_OPA_REQUEST_URL, &opa.connection_string)
            .with_value(
                &AIRFLOW_CORE_AUTH_OPA_CACHE_TTL_IN_SEC,
                opa.cache_entry_time_to_live.as_secs().to_string(),
            )
            .with_value(
                &AIRFLOW_CORE_AUTH_OPA_CACHE_MAXSIZE,
                opa.cache_max_entries.to_string(),
            );
    }

    env_vars
}

fn execution_server_env_vars(cluster: &ValidatedCluster) -> EnvVarSet {
    let mut env_vars = EnvVarSet::new();

    let name = cluster.name_any();
    // The execution API server URL can be any webserver (if there
    // are multiple ones). Parse the list of webservers in a deterministic
    // way by iterating over a BTree map rather than the HashMap.
    if cluster.has_role(&AirflowRole::Webserver) {
        let webserver = role_service_name(&name, &AirflowRole::Webserver.to_string());
        tracing::debug!("Webserver set [{webserver}]");
        // These settings are new in 3.x and will have no affect with earlier versions.
        env_vars = env_vars
            .with_value(
                &AIRFLOW_CORE_EXECUTION_API_SERVER_URL,
                format!("http://{webserver}:{HTTP_PORT}/execution/"),
            )
            .with_value(
                &AIRFLOW_CORE_BASE_URL,
                format!("http://{webserver}:{HTTP_PORT}/"),
            );
    }

    env_vars
}

#[cfg(test)]
mod tests {
    use stackable_operator::shared::time::Duration;

    use super::*;
    use crate::crd::authorization::OpaConfigResolved;

    #[test]
    fn test_constants() {
        // Test that dereferencing the constants does not panic.
        let _ = *ADMIN_EMAIL;
        let _ = *ADMIN_EMAIL_SECRET_KEY;
        let _ = *ADMIN_FIRSTNAME;
        let _ = *ADMIN_FIRSTNAME_SECRET_KEY;
        let _ = *ADMIN_LASTNAME;
        let _ = *ADMIN_LASTNAME_SECRET_KEY;
        let _ = *ADMIN_PASSWORD;
        let _ = *ADMIN_PASSWORD_SECRET_KEY;
        let _ = *ADMIN_USERNAME;
        let _ = *ADMIN_USERNAME_SECRET_KEY;
        let _ = *AIRFLOW_API_AUTH_BACKENDS;
        let _ = *AIRFLOW_API_AUTH_JWT_SECRET;
        let _ = *AIRFLOW_API_SECRET_KEY;
        let _ = *AIRFLOW_API_WORKERS;
        let _ = *AIRFLOW_CELERY_BROKER_URL;
        let _ = *AIRFLOW_CELERY_RESULT_BACKEND;
        let _ = *AIRFLOW_CORE_AUTH_MANAGER;
        let _ = *AIRFLOW_CORE_AUTH_OPA_CACHE_MAXSIZE;
        let _ = *AIRFLOW_CORE_AUTH_OPA_CACHE_TTL_IN_SEC;
        let _ = *AIRFLOW_CORE_AUTH_OPA_REQUEST_URL;
        let _ = *AIRFLOW_CORE_BASE_URL;
        let _ = *AIRFLOW_CORE_DAGS_FOLDER;
        let _ = *AIRFLOW_CORE_EXECUTION_API_SERVER_URL;
        let _ = *AIRFLOW_CORE_EXECUTOR;
        let _ = *AIRFLOW_CORE_FERNET_KEY;
        let _ = *AIRFLOW_CORE_LOAD_EXAMPLES;
        let _ = *AIRFLOW_DATABASE_SQL_ALCHEMY_CONN;
        let _ = *AIRFLOW_FAB_UPDATE_FAB_PERMS;
        let _ = *AIRFLOW_KUBERNETES_EXECUTOR_NAMESPACE;
        let _ = *AIRFLOW_KUBERNETES_EXECUTOR_POD_TEMPLATE_FILE;
        let _ = *AIRFLOW_LOGGING_LOGGING_CONFIG_CLASS;
        let _ = *AIRFLOW_METRICS_STATSD_HOST;
        let _ = *AIRFLOW_METRICS_STATSD_ON;
        let _ = *AIRFLOW_METRICS_STATSD_PORT;
        let _ = *AIRFLOW_SCHEDULER_STANDALONE_DAG_PROCESSOR;
        let _ = *AIRFLOW_WEBSERVER_ENABLE_PROXY_FIX;
        let _ = *AIRFLOW_WEBSERVER_EXPOSE_CONFIG;
        let _ = *AIRFLOW_WEBSERVER_PROXY_FIX_X_FOR;
        let _ = *AIRFLOW_WEBSERVER_SECRET_KEY;
        let _ = *CONTAINERDEBUG_LOG_DIRECTORY;
        let _ = *FORWARDED_ALLOW_IPS;
        let _ = *PYTHONPATH;
        let _ = *STACKABLE_POST_HOOK;
    }

    #[test]
    fn test_airflow_2_authorization_env_vars() {
        let authorization_config = get_test_authorization_config();
        let authorization_env_vars = authorization_env_vars(&authorization_config, "2.10.5");
        let authorization_env_vars = authorization_env_vars
            .into_iter()
            .map(|env| (env.name, env.value.expect("env var value must be present")))
            .collect::<Vec<_>>();

        assert_eq!(
            authorization_env_vars,
            [(
                "AIRFLOW__CORE__AUTH_MANAGER".into(),
                "opa_auth_manager.opa_fab_auth_manager.OpaFabAuthManager".into()
            ),]
        );
    }

    #[test]
    fn test_airflow_3_authorization_env_vars() {
        let authorization_config = get_test_authorization_config();
        let authorization_env_vars = authorization_env_vars(&authorization_config, "3.0.6");
        let authorization_env_vars = authorization_env_vars
            .into_iter()
            .map(|env| (env.name, env.value.expect("env var value must be present")))
            .collect::<Vec<_>>();

        assert_eq!(
            authorization_env_vars,
            [
                (
                    "AIRFLOW__CORE__AUTH_MANAGER".into(),
                    "opa_auth_manager.opa_fab_auth_manager.OpaFabAuthManager".into()
                ),
                (
                    "AIRFLOW__CORE__AUTH_OPA_CACHE_MAXSIZE".into(),
                    "1000".into()
                ),
                (
                    "AIRFLOW__CORE__AUTH_OPA_CACHE_TTL_IN_SEC".into(),
                    "30".into()
                ),
                (
                    "AIRFLOW__CORE__AUTH_OPA_REQUEST_URL".into(),
                    "http://opa-server.default.svc.cluster.local:8081/v1/data/airflow".into()
                ),
            ]
        );
    }

    fn get_test_authorization_config() -> AirflowAuthorizationResolved {
        AirflowAuthorizationResolved {
            opa: Some(OpaConfigResolved {
                connection_string:
                    "http://opa-server.default.svc.cluster.local:8081/v1/data/airflow".to_string(),
                cache_entry_time_to_live: Duration::from_secs(30),
                cache_max_entries: 1000,
            }),
        }
    }
}
