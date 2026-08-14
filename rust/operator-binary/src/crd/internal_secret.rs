use std::str::FromStr;

use stackable_operator::{constant, v2::types::kubernetes::SecretKey};

// Secret key used to run the api server. It should be as random as possible.
// It should be consistent across instances of the webserver. The webserver key
// is also used to authorize requests to Celery workers when logs are retrieved.
constant!(pub INTERNAL_SECRET_SECRET_KEY: SecretKey = "INTERNAL_SECRET");
// Used for env-var: AIRFLOW__API_AUTH__JWT_SECRET
// Secret key used to encode and decode JWTs to authenticate to public and
// private APIs. It should be as random as possible, but consistent across
// instances of API services.
constant!(pub JWT_SECRET_SECRET_KEY: SecretKey = "JWT_SECRET");
// Used for env-var: AIRFLOW__CORE__FERNET_KEY
// See https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/fernet.html#security-fernet
constant!(pub FERNET_KEY_SECRET_KEY: SecretKey = "FERNET_KEY");
