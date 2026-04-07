// Secret key used to run the api server. It should be as random as possible.
// It should be consistent across instances of the webserver. The webserver key
// is also used to authorize requests to Celery workers when logs are retrieved.
pub const INTERNAL_SECRET_SECRET_KEY: &str = "INTERNAL_SECRET";
// Used for env-var: AIRFLOW__API_AUTH__JWT_SECRET
// Secret key used to encode and decode JWTs to authenticate to public and
// private APIs. It should be as random as possible, but consistent across
// instances of API services.
pub const JWT_SECRET_SECRET_KEY: &str = "JWT_SECRET";
// Used for env-var: AIRFLOW__CORE__FERNET_KEY
// See https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/fernet.html#security-fernet
pub const FERNET_KEY_SECRET_KEY: &str = "FERNET_KEY";
