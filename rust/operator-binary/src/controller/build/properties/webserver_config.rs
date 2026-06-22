//! Builds the `webserver_config.py` Flask configuration file from the resolved
//! authentication/authorization config plus user-provided config overrides.

use std::{collections::BTreeMap, io::Write};

use indoc::formatdoc;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    commons::tls_verification::TlsVerification,
    crd::authentication::{ldap, oidc},
    v2::flask_config_writer,
};

use crate::{
    controller::ValidatedCluster,
    crd::{
        AirflowConfigOptions,
        authentication::{
            AirflowAuthenticationClassResolved, AirflowClientAuthenticationDetailsResolved,
            DEFAULT_OIDC_PROVIDER, FlaskRolesSyncMoment,
        },
        authorization::AirflowAuthorizationResolved,
    },
};

const PYTHON_IMPORTS: &[&str] = &[
    "import os",
    "from flask_appbuilder.const import (AUTH_DB, AUTH_LDAP, AUTH_OAUTH, AUTH_REMOTE_USER)",
    "basedir = os.path.abspath(os.path.dirname(__file__))",
    "WTF_CSRF_ENABLED = True",
];

/// Marks arbitrary Python code to prepend verbatim to the generated file.
const CONFIG_OVERRIDE_FILE_HEADER_KEY: &str = "FILE_HEADER";
/// Marks arbitrary Python code to append verbatim to the generated file.
const CONFIG_OVERRIDE_FILE_FOOTER_KEY: &str = "FILE_FOOTER";

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to create LDAP endpoint url."))]
    FailedToCreateLdapEndpointUrl { source: ldap::v1alpha1::Error },

    #[snafu(display("invalid OIDC endpoint"))]
    InvalidOidcEndpoint { source: oidc::v1alpha1::Error },

    #[snafu(display("invalid well-known OIDC configuration URL"))]
    InvalidWellKnownConfigUrl { source: oidc::v1alpha1::Error },

    #[snafu(display("failed to write the webserver config file"))]
    WriteConfigFile {
        source: flask_config_writer::FlaskAppConfigWriterError,
    },

    #[snafu(display("failed to write the header/footer to the webserver config file"))]
    WriteHeaderFooter { source: std::io::Error },
}

/// Renders the `webserver_config.py` contents: operator defaults (derived from the
/// resolved authentication/authorization config) with the user's `config_overrides`
/// applied last, wrapped by the optional `FILE_HEADER`/`FILE_FOOTER` Python blocks.
pub fn build(
    validated_cluster: &ValidatedCluster,
    config_file_overrides: &BTreeMap<String, String>,
) -> Result<String, Error> {
    // this will call default values from AirflowClientAuthenticationDetails
    let mut config = build_airflow_config(
        &validated_cluster.cluster_config.authentication_config,
        &validated_cluster.cluster_config.authorization_config,
        &validated_cluster.image.product_version,
    )?;

    let mut file_config = config_file_overrides.clone();

    // now add any overrides, replacing any defaults
    config.append(&mut file_config);

    let mut config_file = Vec::new();

    // By removing the keys from `config`, we avoid pasting the Python code into a Python variable as well
    // (which would be bad)
    if let Some(header) = config.remove(CONFIG_OVERRIDE_FILE_HEADER_KEY) {
        writeln!(config_file, "{}", header).context(WriteHeaderFooterSnafu)?;
    }

    let temp_file_footer: Option<String> = config.remove(CONFIG_OVERRIDE_FILE_FOOTER_KEY);

    // Workaround for apache-airflow-providers-fab 3.6.x: `_search_ldap` calls
    // `ldap.filter.escape_filter_chars` without ever importing `ldap.filter`, so every LDAP
    // login fails with `AttributeError: module 'ldap' has no attribute 'filter'` (the
    // `filter` submodule is not auto-loaded by `import ldap`). Importing it here registers
    // the submodule on the `ldap` package for the whole webserver process, which makes the
    // FAB code resolve it. Fixed upstream in FAB 3.7.0 (apache/airflow#68226); this import
    // is a harmless no-op there and on any python-ldap version.
    let mut imports = PYTHON_IMPORTS.to_vec();
    let has_ldap = validated_cluster
        .cluster_config
        .authentication_config
        .authentication_classes_resolved
        .iter()
        .any(|auth_class| matches!(auth_class, AirflowAuthenticationClassResolved::Ldap { .. }));
    if has_ldap {
        imports.push("import ldap.filter");
    }

    flask_config_writer::write::<AirflowConfigOptions, _, _>(
        &mut config_file,
        config.iter(),
        &imports,
    )
    .context(WriteConfigFileSnafu)?;

    if let Some(footer) = temp_file_footer {
        writeln!(config_file, "{}", footer).context(WriteHeaderFooterSnafu)?;
    }

    Ok(String::from_utf8(config_file).expect("the Flask config writer only emits valid UTF-8"))
}

/// Builds the operator-default `webserver_config.py` entries derived from the resolved
/// authentication/authorization config.
///
/// Returns the assembled map; the caller applies user `config_overrides` on top.
pub fn build_airflow_config(
    authentication_config: &AirflowClientAuthenticationDetailsResolved,
    authorization_config: &AirflowAuthorizationResolved,
    product_version: &str,
) -> Result<BTreeMap<String, String>> {
    let mut config = BTreeMap::new();

    // should default to AUTH_TYPE = AUTH_DB (an authentication provider may override this below)
    config.insert(AirflowConfigOptions::AuthType.to_string(), "AUTH_DB".into());

    append_authentication_config(&mut config, authentication_config)?;
    append_authorization_config(&mut config, authorization_config, product_version);

    Ok(config)
}

fn append_authentication_config(
    config: &mut BTreeMap<String, String>,
    auth_config: &AirflowClientAuthenticationDetailsResolved,
) -> Result<(), Error> {
    let ldap_providers = auth_config
        .authentication_classes_resolved
        .iter()
        .filter_map(|auth_class| {
            if let AirflowAuthenticationClassResolved::Ldap { provider } = auth_class {
                Some(provider)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    let oidc_providers = auth_config
        .authentication_classes_resolved
        .iter()
        .filter_map(|auth_class| {
            if let AirflowAuthenticationClassResolved::Oidc { provider, oidc } = auth_class {
                Some((provider, oidc))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    if let Some(ldap_provider) = ldap_providers.first() {
        append_ldap_config(config, ldap_provider)?;
    }

    if !oidc_providers.is_empty() {
        append_oidc_config(config, &oidc_providers)?;
    }

    config.insert(
        AirflowConfigOptions::AuthUserRegistration.to_string(),
        auth_config.user_registration.to_string(),
    );
    config.insert(
        AirflowConfigOptions::AuthUserRegistrationRole.to_string(),
        auth_config.user_registration_role.to_string(),
    );
    config.insert(
        AirflowConfigOptions::AuthRolesSyncAtLogin.to_string(),
        (auth_config.sync_roles_at == FlaskRolesSyncMoment::Login).to_string(),
    );

    Ok(())
}

fn append_ldap_config(
    config: &mut BTreeMap<String, String>,
    ldap: &ldap::v1alpha1::AuthenticationProvider,
) -> Result<()> {
    config.insert(
        AirflowConfigOptions::AuthType.to_string(),
        "AUTH_LDAP".into(),
    );
    config.insert(
        AirflowConfigOptions::AuthLdapServer.to_string(),
        ldap.endpoint_url()
            .context(FailedToCreateLdapEndpointUrlSnafu)?
            .to_string(),
    );
    config.insert(
        AirflowConfigOptions::AuthLdapSearch.to_string(),
        ldap.search_base.clone(),
    );
    config.insert(
        AirflowConfigOptions::AuthLdapSearchFilter.to_string(),
        ldap.search_filter.clone(),
    );
    config.insert(
        AirflowConfigOptions::AuthLdapUidField.to_string(),
        ldap.ldap_field_names.uid.clone(),
    );
    config.insert(
        AirflowConfigOptions::AuthLdapGroupField.to_string(),
        ldap.ldap_field_names.group.clone(),
    );
    config.insert(
        AirflowConfigOptions::AuthLdapFirstnameField.to_string(),
        ldap.ldap_field_names.given_name.clone(),
    );
    config.insert(
        AirflowConfigOptions::AuthLdapLastnameField.to_string(),
        ldap.ldap_field_names.surname.clone(),
    );

    // Possible TLS options, see https://github.com/dpgaspar/Flask-AppBuilder/blob/f6f66fc1bcc0163a213e4a2e6f960e91082d201f/flask_appbuilder/security/manager.py#L243-L250
    match &ldap.tls.tls {
        None => {
            config.insert(
                AirflowConfigOptions::AuthLdapTlsDemand.to_string(),
                false.to_string(),
            );
        }
        Some(tls) => {
            config.insert(
                AirflowConfigOptions::AuthLdapTlsDemand.to_string(),
                true.to_string(),
            );
            match &tls.verification {
                TlsVerification::None {} => {
                    config.insert(
                        AirflowConfigOptions::AuthLdapAllowSelfSigned.to_string(),
                        true.to_string(),
                    );
                }
                TlsVerification::Server(_) => {
                    config.insert(
                        AirflowConfigOptions::AuthLdapAllowSelfSigned.to_string(),
                        false.to_string(),
                    );
                    if let Some(ca_path) = ldap.tls.tls_ca_cert_mount_path() {
                        config.insert(
                            AirflowConfigOptions::AuthLdapTlsCacertfile.to_string(),
                            ca_path,
                        );
                    }
                }
            }
        }
    }

    if let Some((username_path, password_path)) = ldap.bind_credentials_mount_paths() {
        config.insert(
            AirflowConfigOptions::AuthLdapBindUser.to_string(),
            format!("open('{username_path}').read()"),
        );
        config.insert(
            AirflowConfigOptions::AuthLdapBindPassword.to_string(),
            format!("open('{password_path}').read()"),
        );
    }

    Ok(())
}

fn append_oidc_config(
    config: &mut BTreeMap<String, String>,
    providers: &[(
        &oidc::v1alpha1::AuthenticationProvider,
        &oidc::v1alpha1::ClientAuthenticationOptions<
            oidc::v1alpha1::ClientAuthenticationMethodOption,
        >,
    )],
) -> Result<(), Error> {
    // Additionally can be set via config
    config.insert(
        AirflowConfigOptions::AuthType.to_string(),
        "AUTH_OAUTH".into(),
    );

    let mut oauth_providers_config = Vec::new();

    for (oidc, client_options) in providers {
        let (env_client_id, env_client_secret) =
            oidc::v1alpha1::AuthenticationProvider::client_credentials_env_names(
                &client_options.client_credentials_secret_ref,
            );
        let mut scopes = oidc.scopes.clone();
        scopes.extend_from_slice(&client_options.extra_scopes);

        let oidc_provider = oidc
            .provider_hint
            .as_ref()
            .unwrap_or(&DEFAULT_OIDC_PROVIDER);

        let oauth_providers_config_entry = match oidc_provider {
            oidc::v1alpha1::IdentityProviderHint::Keycloak => {
                let endpoint_url = oidc.endpoint_url().context(InvalidOidcEndpointSnafu)?;
                let mut api_base_url = endpoint_url.as_str().trim_end_matches('/').to_owned();
                api_base_url.push_str("/protocol/");
                let well_known_config_url = oidc
                    .well_known_config_url()
                    .context(InvalidWellKnownConfigUrlSnafu)?;

                let client_auth_method = client_options
                    .product_specific_fields
                    .client_authentication_method
                    .as_ref();

                formatdoc!(
                    "
                      {{ 'name': 'keycloak',
                        'icon': 'fa-key',
                        'token_key': 'access_token',
                        'remote_app': {{
                          'client_id': os.environ.get('{env_client_id}'),
                          'client_secret': os.environ.get('{env_client_secret}'),
                          'client_kwargs': {{
                            'scope': '{scopes}'
                          }},
                          'api_base_url': '{api_base_url}',
                          'server_metadata_url': '{well_known_config_url}',
                          'token_endpoint_auth_method': '{client_auth_method}',
                        }},
                      }}",
                    scopes = scopes.join(" "),
                )
            }
        };

        oauth_providers_config.push(oauth_providers_config_entry);
    }

    config.insert(
        AirflowConfigOptions::OauthProviders.to_string(),
        formatdoc!(
            "[
             {joined_oauth_providers_config}
             ]
             ",
            joined_oauth_providers_config = oauth_providers_config.join(",\n")
        ),
    );

    Ok(())
}

fn append_authorization_config(
    config: &mut BTreeMap<String, String>,
    authorization_config: &AirflowAuthorizationResolved,
    product_version: &str,
) {
    // See `env_vars::authorization_env_vars` for why we only care about Airflow 2
    if !product_version.starts_with("2.") {
        return;
    }
    let Some(opa_config) = &authorization_config.opa else {
        return;
    };

    config.extend([
        (
            AirflowConfigOptions::AuthOpaRequestUrl.to_string(),
            opa_config.connection_string.to_owned(),
        ),
        (
            AirflowConfigOptions::AuthOpaCacheTtlInSec.to_string(),
            opa_config.cache_entry_time_to_live.as_secs().to_string(),
        ),
        (
            AirflowConfigOptions::AuthOpaCacheMaxsize.to_string(),
            opa_config.cache_max_entries.to_string(),
        ),
    ]);
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use indoc::formatdoc;
    use rstest::rstest;
    use stackable_operator::{
        crd::authentication::{ldap, oidc},
        shared::time::Duration,
    };

    use super::build_airflow_config;
    use crate::crd::{
        authentication::{
            AirflowAuthenticationClassResolved, AirflowClientAuthenticationDetailsResolved,
            FlaskRolesSyncMoment, default_user_registration,
        },
        authorization::{AirflowAuthorizationResolved, OpaConfigResolved},
    };

    const TEST_AIRFLOW_VERSION: &str = "3.0.6";

    #[test]
    fn test_auth_db_config() {
        let authentication_config = AirflowClientAuthenticationDetailsResolved {
            authentication_classes_resolved: vec![],
            user_registration: true,
            user_registration_role: "User".to_string(),
            sync_roles_at: FlaskRolesSyncMoment::Registration,
        };

        let authorization_config = AirflowAuthorizationResolved { opa: None };

        let result = build_airflow_config(
            &authentication_config,
            &authorization_config,
            TEST_AIRFLOW_VERSION,
        )
        .expect("Ok");

        assert_eq!(
            BTreeMap::from([
                ("AUTH_ROLES_SYNC_AT_LOGIN".into(), "false".into()),
                ("AUTH_TYPE".into(), "AUTH_DB".into()),
                ("AUTH_USER_REGISTRATION".into(), "true".into()),
                ("AUTH_USER_REGISTRATION_ROLE".into(), "User".into())
            ]),
            result
        );
    }

    #[test]
    fn test_ldap_config() {
        let ldap_provider_yaml = r#"
            hostname: openldap.default.svc.cluster.local
            port: 636
            searchBase: ou=users,dc=example,dc=org
            ldapFieldNames:
              uid: uid
            bindCredentials:
              secretClass: airflow-with-ldap-server-veri-tls-ldap-bind
            tls:
              verification:
                server:
                  caCert:
                    secretClass: openldap-tls
        "#;
        let deserializer = serde_yaml::Deserializer::from_str(ldap_provider_yaml);
        let ldap_provider: ldap::v1alpha1::AuthenticationProvider =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();

        let authentication_config = AirflowClientAuthenticationDetailsResolved {
            authentication_classes_resolved: vec![AirflowAuthenticationClassResolved::Ldap {
                provider: ldap_provider,
            }],
            user_registration: true,
            user_registration_role: "Admin".to_string(),
            sync_roles_at: FlaskRolesSyncMoment::Registration,
        };

        let authorization_config = AirflowAuthorizationResolved { opa: None };

        let result = build_airflow_config(
            &authentication_config,
            &authorization_config,
            TEST_AIRFLOW_VERSION,
        )
        .expect("Ok");

        assert_eq!(BTreeMap::from([
            ("AUTH_LDAP_ALLOW_SELF_SIGNED".into(), "false".into()),
            ("AUTH_LDAP_BIND_PASSWORD".into(), "open('/stackable/secrets/airflow-with-ldap-server-veri-tls-ldap-bind/password').read()".into()),
            ("AUTH_LDAP_BIND_USER".into(), "open('/stackable/secrets/airflow-with-ldap-server-veri-tls-ldap-bind/user').read()".into()),
            ("AUTH_LDAP_FIRSTNAME_FIELD".into(), "givenName".into()),
            ("AUTH_LDAP_GROUP_FIELD".into(), "memberof".into()),
            ("AUTH_LDAP_LASTNAME_FIELD".into(), "sn".into()),
            ("AUTH_LDAP_SEARCH".into(), "ou=users,dc=example,dc=org".into()),
            ("AUTH_LDAP_SEARCH_FILTER".into(), "".into()),
            ("AUTH_LDAP_SERVER".into(), "ldaps://openldap.default.svc.cluster.local:636".into()),
            ("AUTH_LDAP_TLS_CACERTFILE".into(), "/stackable/secrets/openldap-tls/ca.crt".into()),
            ("AUTH_LDAP_TLS_DEMAND".into(), "true".into()),
            ("AUTH_LDAP_UID_FIELD".into(), "uid".into()),
            ("AUTH_ROLES_SYNC_AT_LOGIN".into(), "false".into()),
            ("AUTH_TYPE".into(), "AUTH_LDAP".into()),
            ("AUTH_USER_REGISTRATION".into(), "true".into()),
            ("AUTH_USER_REGISTRATION_ROLE".into(), "Admin".into())
        ]), result);
    }

    #[rstest]
    #[case("/realms/sdp")]
    #[case("/realms/sdp/")]
    #[case("/realms/sdp/////")]
    fn test_oidc_config(#[case] root_path: &str) {
        let oidc_provider_yaml1 = formatdoc!(
            "hostname: my.keycloak1.server
            port: 12345
            rootPath: {root_path}
            tls:
              verification:
                server:
                  caCert:
                    secretClass: keycloak-ca-cert
            principalClaim: preferred_username
            scopes:
              - openid
              - email
              - profile
            provider_hint: Keycloak
        "
        );
        let deserializer = serde_yaml::Deserializer::from_str(&oidc_provider_yaml1);
        let oidc_provider1: oidc::v1alpha1::AuthenticationProvider =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();

        let oidc_provider_yaml2 = r#"
            hostname: my.keycloak2.server
            principalClaim: preferred_username
            scopes:
              - openid
            provider_hint: Keycloak
        "#;
        let deserializer = serde_yaml::Deserializer::from_str(oidc_provider_yaml2);
        let oidc_provider2: oidc::v1alpha1::AuthenticationProvider =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();

        let authentication_config = AirflowClientAuthenticationDetailsResolved {
            authentication_classes_resolved: vec![
                AirflowAuthenticationClassResolved::Oidc {
                    provider: oidc_provider1,
                    oidc: oidc::v1alpha1::ClientAuthenticationOptions {
                        client_credentials_secret_ref: "test-client-secret1".to_string(),
                        extra_scopes: vec!["roles".to_string()],
                        product_specific_fields: oidc::v1alpha1::ClientAuthenticationMethodOption {
                            client_authentication_method: Default::default(),
                        },
                    },
                },
                AirflowAuthenticationClassResolved::Oidc {
                    provider: oidc_provider2,
                    oidc: oidc::v1alpha1::ClientAuthenticationOptions {
                        client_credentials_secret_ref: "test-client-secret2".to_string(),
                        extra_scopes: vec![],
                        product_specific_fields: oidc::v1alpha1::ClientAuthenticationMethodOption {
                            client_authentication_method: Default::default(),
                        },
                    },
                },
            ],
            user_registration: default_user_registration(),
            user_registration_role: "Admin".to_string(),
            sync_roles_at: FlaskRolesSyncMoment::Registration,
        };

        let authorization_config = AirflowAuthorizationResolved { opa: None };

        let result = build_airflow_config(
            &authentication_config,
            &authorization_config,
            TEST_AIRFLOW_VERSION,
        )
        .expect("Ok");

        assert_eq!(
            BTreeMap::from([
                ("AUTH_ROLES_SYNC_AT_LOGIN".into(), "false".into()),
                ("AUTH_TYPE".into(), "AUTH_OAUTH".into()),
                ("AUTH_USER_REGISTRATION".into(), "true".into()),
                ("AUTH_USER_REGISTRATION_ROLE".into(), "Admin".into()),
                (
                    "OAUTH_PROVIDERS".into(),
                    formatdoc! {"
              [
              {{ 'name': 'keycloak',
                'icon': 'fa-key',
                'token_key': 'access_token',
                'remote_app': {{
                  'client_id': os.environ.get('OIDC_A96BCC4FA49835D2_CLIENT_ID'),
                  'client_secret': os.environ.get('OIDC_A96BCC4FA49835D2_CLIENT_SECRET'),
                  'client_kwargs': {{
                    'scope': 'openid email profile roles'
                  }},
                  'api_base_url': 'https://my.keycloak1.server:12345/realms/sdp/protocol/',
                  'server_metadata_url': 'https://my.keycloak1.server:12345/realms/sdp/.well-known/openid-configuration',
                  'token_endpoint_auth_method': 'client_secret_basic',
                }},
              }},
              {{ 'name': 'keycloak',
                'icon': 'fa-key',
                'token_key': 'access_token',
                'remote_app': {{
                  'client_id': os.environ.get('OIDC_3A305E38C3B561F3_CLIENT_ID'),
                  'client_secret': os.environ.get('OIDC_3A305E38C3B561F3_CLIENT_SECRET'),
                  'client_kwargs': {{
                    'scope': 'openid'
                  }},
                  'api_base_url': 'http://my.keycloak2.server/protocol/',
                  'server_metadata_url': 'http://my.keycloak2.server/.well-known/openid-configuration',
                  'token_endpoint_auth_method': 'client_secret_basic',
                }},
              }}
              ]
              "}
                )
            ]),
            result
        );
    }

    #[test]
    fn test_opa_config() {
        let authentication_config = AirflowClientAuthenticationDetailsResolved {
            authentication_classes_resolved: vec![],
            user_registration: true,
            user_registration_role: "User".to_string(),
            sync_roles_at: FlaskRolesSyncMoment::Registration,
        };

        let authorization_config = AirflowAuthorizationResolved {
            opa: Some(OpaConfigResolved {
                connection_string: "http://opa:8081/v1/data/airflow".to_string(),
                cache_entry_time_to_live: Duration::from_secs(30),
                cache_max_entries: 1000,
            }),
        };

        let result = build_airflow_config(
            &authentication_config,
            &authorization_config,
            TEST_AIRFLOW_VERSION,
        )
        .expect("Ok");

        assert_eq!(
            BTreeMap::from([
                ("AUTH_ROLES_SYNC_AT_LOGIN".into(), "false".into()),
                ("AUTH_TYPE".into(), "AUTH_DB".into()),
                ("AUTH_USER_REGISTRATION".into(), "true".into()),
                ("AUTH_USER_REGISTRATION_ROLE".into(), "User".into())
            ]),
            result
        );
    }
}
