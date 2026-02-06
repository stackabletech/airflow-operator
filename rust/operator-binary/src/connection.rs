use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::EnvVar;

use crate::{
    connections::{database::DbType, queue::QueueType},
    crd::v1alpha2,
    util::env_var_from_secret,
};

const AIRFLOW_DATABASE_SQL_ALCHEMY_CONN: &str = "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN";
const AIRFLOW_CELERY_RESULT_BACKEND: &str = "AIRFLOW__CELERY__RESULT_BACKEND";
const AIRFLOW_CELERY_BROKER_URL: &str = "AIRFLOW__CELERY__BROKER_URL";

// metadata credentials
const META_DB_USERNAME: &str = "META_DB_USERNAME";
const META_DB_PASSWORD: &str = "META_DB_PASSWORD";
// celery backend credentials
const CELERY_DB_USERNAME: &str = "CELERY_DB_USERNAME";
const CELERY_DB_PASSWORD: &str = "CELERY_DB_PASSWORD";
// celery broker credentials
const CELERY_BROKER_USERNAME: &str = "CELERY_BROKER_USERNAME";
const CELERY_BROKER_PASSWORD: &str = "CELERY_BROKER_PASSWORD";
// secret fields
const USERNAME_FIELD: &str = "username";
const PASSWORD_FIELD: &str = "password";
const URI_FIELD: &str = "uri";

pub fn add_metadata_credentials(
    airflow: &v1alpha2::AirflowCluster,
    env: &mut BTreeMap<String, EnvVar>,
) {
    let db_type = &airflow.spec.cluster_config.metadata_database;
    let db_secret = db_type.credentials_secret();

    match db_type {
        DbType::Postgresql(db) => {
            add_typed_credentials(
                env,
                &db_secret,
                META_DB_USERNAME,
                META_DB_PASSWORD,
                AIRFLOW_DATABASE_SQL_ALCHEMY_CONN,
                db.connection_string_alchemy(META_DB_USERNAME, META_DB_PASSWORD),
            );
        }
        DbType::Generic(_) => {
            add_generic_credentials(env, &db_secret, AIRFLOW_DATABASE_SQL_ALCHEMY_CONN);
        }
    }
}

pub fn add_celery_backend_credentials(
    celery_result_backend: &DbType,
    env: &mut BTreeMap<String, EnvVar>,
) {
    let db_secret = celery_result_backend.credentials_secret();

    match celery_result_backend {
        DbType::Postgresql(db) => {
            add_typed_credentials(
                env,
                &db_secret,
                CELERY_DB_USERNAME,
                CELERY_DB_PASSWORD,
                AIRFLOW_CELERY_RESULT_BACKEND,
                db.connection_string_celery(CELERY_DB_USERNAME, CELERY_DB_PASSWORD),
            );
        }
        DbType::Generic(_) => {
            add_generic_credentials(env, &db_secret, AIRFLOW_CELERY_RESULT_BACKEND);
        }
    }
}

pub fn add_celery_broker_credentials(
    celery_broker_url: &QueueType,
    env: &mut BTreeMap<String, EnvVar>,
) {
    let queue_secret = celery_broker_url.credentials_secret();

    match celery_broker_url {
        QueueType::Redis(queue) => {
            add_typed_credentials(
                env,
                &queue_secret,
                CELERY_BROKER_USERNAME,
                CELERY_BROKER_PASSWORD,
                AIRFLOW_CELERY_BROKER_URL,
                queue.connection_string(CELERY_BROKER_USERNAME, CELERY_BROKER_PASSWORD),
            );
        }
        QueueType::Generic(_) => {
            add_generic_credentials(env, &queue_secret, AIRFLOW_CELERY_BROKER_URL);
        }
    }
}

fn add_typed_credentials(
    env: &mut BTreeMap<String, EnvVar>,
    secret: &str,
    username_key: &str,
    password_key: &str,
    connection_key: &str,
    connection_string: String,
) {
    // Add username and password from secret
    add_secret_env_vars(
        env,
        secret,
        &[
            (username_key, USERNAME_FIELD),
            (password_key, PASSWORD_FIELD),
        ],
    );

    // Build connection string using username/password env vars
    env.insert(
        connection_key.into(),
        EnvVar {
            name: connection_key.into(),
            value: Some(connection_string),
            ..Default::default()
        },
    );
}

fn add_generic_credentials(env: &mut BTreeMap<String, EnvVar>, secret: &str, connection_key: &str) {
    env.insert(
        connection_key.into(),
        env_var_from_secret(connection_key, secret, URI_FIELD),
    );
}

fn add_secret_env_vars(env: &mut BTreeMap<String, EnvVar>, secret: &str, vars: &[(&str, &str)]) {
    for (env_key, secret_key) in vars {
        env.insert(
            (*env_key).into(),
            env_var_from_secret(env_key, secret, secret_key),
        );
    }
}
