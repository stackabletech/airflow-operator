//! OpenLineage lineage-emission wiring for Airflow.
//!
//! The reusable CRD and resolution types live in operator-rs
//! ([`stackable_operator::crd::openlineage`]). This module resolves an
//! [`OpenLineageJob`] into the Airflow-specific set of environment variables (and TLS CA volumes)
//! needed to configure the
//! [`apache-airflow-providers-openlineage`](https://airflow.apache.org/docs/apache-airflow-providers-openlineage/stable/)
//! provider.
//!
//! Airflow configures OpenLineage entirely via environment variables (this operator renders no
//! `airflow.cfg`). The provider resolves its transport from `AIRFLOW__OPENLINEAGE__TRANSPORT`
//! (a JSON string) or `AIRFLOW__OPENLINEAGE__CONFIG_PATH`, and only falls back to the OpenLineage
//! Python client's own `OPENLINEAGE__TRANSPORT__*` env vars when neither is set. We deliberately use
//! that fallback path (leaving the Airflow-native transport keys unset) so the API key can be
//! injected from a Secret via `secretKeyRef` instead of being embedded in a plaintext JSON value.
//!
//! The Python client's HTTP transport has no CA-path option (only a boolean `verify`), so a
//! SecretClass-provided CA is trusted by mounting it and pointing `REQUESTS_CA_BUNDLE` at it.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    commons::tls_verification::TlsClientDetailsError,
    crd::{
        authentication::core::v1alpha1::{AuthenticationClass, AuthenticationClassProvider},
        openlineage::{
            ResolvedOpenLineageConnection,
            v1alpha1::{OpenLineageError, OpenLineageJob},
        },
    },
    k8s_openapi::api::core::v1::{EnvVar, Volume, VolumeMount},
};

use crate::util::env_var_from_secret;

// OpenLineage Python client transport env vars. These are the Airflow provider's fallback config
// source, used only when no Airflow-native transport (`AIRFLOW__OPENLINEAGE__TRANSPORT` /
// `__CONFIG_PATH`) is configured - which is exactly the state this operator leaves them in.
const OPENLINEAGE_TRANSPORT_TYPE: &str = "OPENLINEAGE__TRANSPORT__TYPE";
const OPENLINEAGE_TRANSPORT_URL: &str = "OPENLINEAGE__TRANSPORT__URL";
const OPENLINEAGE_TRANSPORT_ENDPOINT: &str = "OPENLINEAGE__TRANSPORT__ENDPOINT";
const OPENLINEAGE_TRANSPORT_VERIFY: &str = "OPENLINEAGE__TRANSPORT__VERIFY";
const OPENLINEAGE_TRANSPORT_AUTH_TYPE: &str = "OPENLINEAGE__TRANSPORT__AUTH__TYPE";
const OPENLINEAGE_TRANSPORT_AUTH_API_KEY: &str = "OPENLINEAGE__TRANSPORT__AUTH__API_KEY";

/// Airflow-native OpenLineage namespace setting. This is orthogonal to the transport lookup and is
/// applied regardless of where the transport config comes from.
const AIRFLOW_OPENLINEAGE_NAMESPACE: &str = "AIRFLOW__OPENLINEAGE__NAMESPACE";

/// Standard `requests`/Python CA-bundle env var. Used to trust a SecretClass-provided CA, since the
/// OpenLineage Python HTTP transport has no CA-path option (only a boolean `verify`).
const REQUESTS_CA_BUNDLE: &str = "REQUESTS_CA_BUNDLE";

const OPENLINEAGE_TRANSPORT_TYPE_HTTP: &str = "http";
const OPENLINEAGE_ENDPOINT_LINEAGE: &str = "api/v1/lineage";
const OPENLINEAGE_AUTH_TYPE_API_KEY: &str = "api_key";

/// Fixed Secret key that must hold the OpenLineage HTTP transport API key / bearer token.
pub const OPENLINEAGE_AUTH_SECRET_KEY: &str = "apiKey";

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve OpenLineage connection"))]
    ResolveConnection { source: OpenLineageError },

    #[snafu(display("failed to resolve OpenLineage AuthenticationClass"))]
    ResolveAuthenticationClass { source: OpenLineageError },

    #[snafu(display(
        "the AuthenticationClass provider {provider:?} is not supported for OpenLineage. \
         Only the 'static' provider is supported."
    ))]
    UnsupportedAuthenticationProvider { provider: String },

    #[snafu(display("failed to build TLS volumes and mounts for the OpenLineage connection"))]
    TlsVolumesAndMounts { source: TlsClientDetailsError },
}

/// The resolved, Airflow-specific OpenLineage configuration: the environment variables to add to
/// every workload container, plus any TLS CA volume/mount required to trust the backend.
#[derive(Clone, Debug, Default)]
pub struct ResolvedOpenLineageConfig {
    /// Environment variables configuring the OpenLineage transport (and, when authenticated, the
    /// Secret-backed API key). Injected into the statefulset roles and the Kubernetes-executor
    /// pod template.
    pub env_vars: Vec<EnvVar>,
    /// Volumes for a SecretClass-provided CA certificate (empty unless TLS server verification with
    /// a SecretClass is configured).
    pub volumes: Vec<Volume>,
    /// Volume mounts for the CA certificate above.
    pub volume_mounts: Vec<VolumeMount>,
}

impl ResolvedOpenLineageConfig {
    /// Resolves an [`OpenLineageJob`] (inline connection or `OpenLineageConnection` reference,
    /// plus an optional `AuthenticationClass`) into the Airflow-specific configuration.
    pub async fn from_config(
        open_lineage: &OpenLineageJob,
        client: &Client,
        namespace: &str,
    ) -> Result<Self, Error> {
        let connection = open_lineage
            .connection
            .clone()
            .resolve(client, namespace)
            .await
            .context(ResolveConnectionSnafu)?;

        let auth_secret_name = match connection
            .resolve_authentication_class(client)
            .await
            .context(ResolveAuthenticationClassSnafu)?
        {
            Some(auth_class) => Some(static_provider_secret_name(&auth_class)?),
            None => None,
        };

        Self::build(&connection, open_lineage, namespace, auth_secret_name)
    }

    /// Assembles the configuration from an already-resolved connection and (optional) auth Secret
    /// name. Kept separate from [`Self::from_config`] so it can be unit tested without a client.
    fn build(
        connection: &ResolvedOpenLineageConnection,
        open_lineage: &OpenLineageJob,
        namespace: &str,
        auth_secret_name: Option<String>,
    ) -> Result<Self, Error> {
        let mut env_vars = Vec::new();
        let mut volumes = Vec::new();
        let mut volume_mounts = Vec::new();

        // Transport (delivered via the OpenLineage Python client fallback env vars).
        env_vars.push(plain_env(
            OPENLINEAGE_TRANSPORT_TYPE,
            OPENLINEAGE_TRANSPORT_TYPE_HTTP,
        ));
        env_vars.push(plain_env(
            OPENLINEAGE_TRANSPORT_URL,
            &connection.transport_url(),
        ));
        env_vars.push(plain_env(
            OPENLINEAGE_TRANSPORT_ENDPOINT,
            OPENLINEAGE_ENDPOINT_LINEAGE,
        ));

        // Namespace: the explicit value, else the workload's Kubernetes namespace.
        let ol_namespace = open_lineage
            .namespace
            .clone()
            .unwrap_or_else(|| namespace.to_string());
        env_vars.push(plain_env(AIRFLOW_OPENLINEAGE_NAMESPACE, &ol_namespace));

        // TLS handling.
        if connection.tls.uses_tls_verification() {
            // Server verification. A public (WebPki) CA needs nothing; a SecretClass CA must be
            // mounted and trusted via REQUESTS_CA_BUNDLE.
            if let Some(ca_cert_path) = connection.tls.tls_ca_cert_mount_path() {
                let (tls_volumes, tls_mounts) = connection
                    .tls
                    .volumes_and_mounts()
                    .context(TlsVolumesAndMountsSnafu)?;
                volumes.extend(tls_volumes);
                volume_mounts.extend(tls_mounts);
                env_vars.push(plain_env(REQUESTS_CA_BUNDLE, &ca_cert_path));
            }
        } else if connection.tls.uses_tls() {
            // TLS configured but verification disabled.
            env_vars.push(plain_env(OPENLINEAGE_TRANSPORT_VERIFY, "false"));
        }

        // Authentication (Static AuthenticationClass only). The token is delivered from a Secret via
        // secretKeyRef so it never lands in the CRD, ConfigMap or a plaintext env value.
        if let Some(secret_name) = auth_secret_name {
            env_vars.push(plain_env(
                OPENLINEAGE_TRANSPORT_AUTH_TYPE,
                OPENLINEAGE_AUTH_TYPE_API_KEY,
            ));
            env_vars.push(env_var_from_secret(
                OPENLINEAGE_TRANSPORT_AUTH_API_KEY,
                &secret_name,
                OPENLINEAGE_AUTH_SECRET_KEY,
            ));
        }

        // Airflow has no global OpenLineage job name (names are derived per DAG/task), so the shared
        // `appName` field is not applicable and is ignored.
        if open_lineage.app_name.is_some() {
            tracing::debug!(
                "The OpenLineage `appName` field is not used by Airflow and will be ignored; \
                 Airflow derives OpenLineage job names per DAG/task."
            );
        }

        Ok(Self {
            env_vars,
            volumes,
            volume_mounts,
        })
    }
}

/// Extracts the user-credentials Secret name from a `static` [`AuthenticationClass`], rejecting any
/// other provider (only the static provider is supported for OpenLineage).
fn static_provider_secret_name(auth_class: &AuthenticationClass) -> Result<String, Error> {
    match &auth_class.spec.provider {
        AuthenticationClassProvider::Static(provider) => {
            Ok(provider.user_credentials_secret.name.clone())
        }
        other => UnsupportedAuthenticationProviderSnafu {
            provider: other.to_string(),
        }
        .fail(),
    }
}

fn plain_env(name: &str, value: &str) -> EnvVar {
    EnvVar {
        name: name.to_string(),
        value: Some(value.to_string()),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use stackable_operator::{
        commons::tls_verification::{
            CaCert, Tls, TlsClientDetails, TlsServerVerification, TlsVerification,
        },
        crd::{
            authentication::{
                core::v1alpha1::{AuthenticationClass, AuthenticationClassProvider},
                ldap, r#static,
            },
            openlineage::v1alpha1::{
                InlineConnectionOrReference, OpenLineageConnectionSpec, OpenLineageJob,
            },
        },
    };

    use super::*;

    const NAMESPACE: &str = "default";

    fn connection(tls: TlsClientDetails) -> OpenLineageConnectionSpec {
        OpenLineageConnectionSpec {
            host: "marquez".to_string(),
            port: 5000,
            tls,
            authentication_class_ref: None,
        }
    }

    fn job(connection: OpenLineageConnectionSpec, namespace: Option<String>) -> OpenLineageJob {
        OpenLineageJob {
            connection: InlineConnectionOrReference::Inline(connection),
            namespace,
            app_name: None,
        }
    }

    fn env_value<'a>(config: &'a ResolvedOpenLineageConfig, name: &str) -> Option<&'a str> {
        config
            .env_vars
            .iter()
            .find(|e| e.name == name)
            .and_then(|e| e.value.as_deref())
    }

    fn no_tls() -> TlsClientDetails {
        TlsClientDetails { tls: None }
    }

    fn secret_class_tls() -> TlsClientDetails {
        TlsClientDetails {
            tls: Some(Tls {
                verification: TlsVerification::Server(TlsServerVerification {
                    ca_cert: CaCert::SecretClass("openlineage-cert".to_string()),
                }),
            }),
        }
    }

    fn web_pki_tls() -> TlsClientDetails {
        TlsClientDetails {
            tls: Some(Tls {
                verification: TlsVerification::Server(TlsServerVerification {
                    ca_cert: CaCert::WebPki {},
                }),
            }),
        }
    }

    fn no_verification_tls() -> TlsClientDetails {
        TlsClientDetails {
            tls: Some(Tls {
                verification: TlsVerification::None {},
            }),
        }
    }

    #[test]
    fn plain_http_transport_without_tls_or_auth() {
        let config = ResolvedOpenLineageConfig::build(
            &connection(no_tls()),
            &job(connection(no_tls()), None),
            NAMESPACE,
            None,
        )
        .unwrap();

        assert_eq!(env_value(&config, OPENLINEAGE_TRANSPORT_TYPE), Some("http"));
        assert_eq!(
            env_value(&config, OPENLINEAGE_TRANSPORT_URL),
            Some("http://marquez:5000")
        );
        assert_eq!(
            env_value(&config, OPENLINEAGE_TRANSPORT_ENDPOINT),
            Some("api/v1/lineage")
        );
        assert_eq!(
            env_value(&config, AIRFLOW_OPENLINEAGE_NAMESPACE),
            Some("default")
        );
        assert!(config.volumes.is_empty());
        assert!(config.volume_mounts.is_empty());
        assert!(env_value(&config, OPENLINEAGE_TRANSPORT_AUTH_TYPE).is_none());
        assert!(env_value(&config, REQUESTS_CA_BUNDLE).is_none());
        assert!(env_value(&config, OPENLINEAGE_TRANSPORT_VERIFY).is_none());
    }

    #[test]
    fn explicit_namespace_overrides_kubernetes_namespace() {
        let conn = connection(no_tls());
        let config = ResolvedOpenLineageConfig::build(
            &conn,
            &job(conn.clone(), Some("lineage-ns".to_string())),
            NAMESPACE,
            None,
        )
        .unwrap();

        assert_eq!(
            env_value(&config, AIRFLOW_OPENLINEAGE_NAMESPACE),
            Some("lineage-ns")
        );
    }

    #[test]
    fn secret_class_tls_mounts_ca_and_sets_bundle() {
        let conn = connection(secret_class_tls());
        let config =
            ResolvedOpenLineageConfig::build(&conn, &job(conn.clone(), None), NAMESPACE, None)
                .unwrap();

        assert_eq!(
            env_value(&config, OPENLINEAGE_TRANSPORT_URL),
            Some("https://marquez:5000")
        );
        assert_eq!(
            env_value(&config, REQUESTS_CA_BUNDLE),
            Some("/stackable/secrets/openlineage-cert/ca.crt")
        );
        assert_eq!(config.volumes.len(), 1);
        assert_eq!(config.volume_mounts.len(), 1);
        assert!(env_value(&config, OPENLINEAGE_TRANSPORT_VERIFY).is_none());
    }

    #[test]
    fn web_pki_tls_uses_https_without_mounts() {
        let conn = connection(web_pki_tls());
        let config =
            ResolvedOpenLineageConfig::build(&conn, &job(conn.clone(), None), NAMESPACE, None)
                .unwrap();

        assert_eq!(
            env_value(&config, OPENLINEAGE_TRANSPORT_URL),
            Some("https://marquez:5000")
        );
        assert!(config.volumes.is_empty());
        assert!(env_value(&config, REQUESTS_CA_BUNDLE).is_none());
        assert!(env_value(&config, OPENLINEAGE_TRANSPORT_VERIFY).is_none());
    }

    #[test]
    fn tls_without_verification_disables_verify() {
        let conn = connection(no_verification_tls());
        let config =
            ResolvedOpenLineageConfig::build(&conn, &job(conn.clone(), None), NAMESPACE, None)
                .unwrap();

        assert_eq!(
            env_value(&config, OPENLINEAGE_TRANSPORT_VERIFY),
            Some("false")
        );
        assert!(config.volumes.is_empty());
        assert!(env_value(&config, REQUESTS_CA_BUNDLE).is_none());
    }

    #[test]
    fn auth_adds_secret_backed_api_key() {
        let conn = connection(no_tls());
        let config = ResolvedOpenLineageConfig::build(
            &conn,
            &job(conn.clone(), None),
            NAMESPACE,
            Some("openlineage-auth-secret".to_string()),
        )
        .unwrap();

        assert_eq!(
            env_value(&config, OPENLINEAGE_TRANSPORT_AUTH_TYPE),
            Some("api_key")
        );

        let api_key = config
            .env_vars
            .iter()
            .find(|e| e.name == OPENLINEAGE_TRANSPORT_AUTH_API_KEY)
            .expect("api key env var must be present");
        // The token is Secret-backed: never a plaintext value.
        assert!(api_key.value.is_none());
        let secret_key_ref = api_key
            .value_from
            .as_ref()
            .and_then(|s| s.secret_key_ref.as_ref())
            .expect("api key must be a secretKeyRef");
        assert_eq!(secret_key_ref.name, "openlineage-auth-secret");
        assert_eq!(secret_key_ref.key, OPENLINEAGE_AUTH_SECRET_KEY);
    }

    #[test]
    fn static_provider_secret_name_extracts_name() {
        let auth_class = AuthenticationClass::new(
            "openlineage-auth",
            stackable_operator::crd::authentication::core::v1alpha1::AuthenticationClassSpec {
                provider: AuthenticationClassProvider::Static(
                    r#static::v1alpha1::AuthenticationProvider {
                        user_credentials_secret: r#static::v1alpha1::UserCredentialsSecretRef {
                            name: "my-secret".to_string(),
                        },
                    },
                ),
            },
        );

        assert_eq!(
            static_provider_secret_name(&auth_class).unwrap(),
            "my-secret"
        );
    }

    #[test]
    fn non_static_provider_is_rejected() {
        let ldap_provider: ldap::v1alpha1::AuthenticationProvider =
            serde_json::from_value(serde_json::json!({ "hostname": "my.ldap.server" }))
                .expect("valid minimal LDAP provider");
        let auth_class = AuthenticationClass::new(
            "openlineage-auth",
            stackable_operator::crd::authentication::core::v1alpha1::AuthenticationClassSpec {
                provider: AuthenticationClassProvider::Ldap(ldap_provider),
            },
        );

        let err = static_provider_secret_name(&auth_class).unwrap_err();
        assert!(matches!(
            err,
            Error::UnsupportedAuthenticationProvider { .. }
        ));
    }
}
