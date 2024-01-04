use snafu::{ResultExt, Snafu};
use stackable_airflow_crd::{
    authentication::AirflowAuthenticationConfigResolved, authentication::FlaskRolesSyncMoment,
    AirflowConfigOptions,
};
use stackable_operator::commons::authentication::{
    ldap::AuthenticationProvider, tls::TlsVerification, AuthenticationClassProvider,
};
use std::collections::BTreeMap;

pub const PYTHON_IMPORTS: &[&str] = &[
    "import os",
    "from flask_appbuilder.const import (AUTH_DB, AUTH_LDAP, AUTH_OAUTH, AUTH_OID, AUTH_REMOTE_USER)",
    "basedir = os.path.abspath(os.path.dirname(__file__))",
    "WTF_CSRF_ENABLED = True",
];

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to create LDAP endpoint url."))]
    FailedToCreateLdapEndpointUrl {
        source: stackable_operator::commons::authentication::ldap::Error,
    },
}

pub fn add_airflow_config(
    config: &mut BTreeMap<String, String>,
    authentication_config: &Vec<AirflowAuthenticationConfigResolved>,
) -> Result<()> {
    if !config.contains_key(&*AirflowConfigOptions::AuthType.to_string()) {
        config.insert(
            // should default to AUTH_TYPE = AUTH_DB
            AirflowConfigOptions::AuthType.to_string(),
            "AUTH_DB".into(),
        );
    }

    append_authentication_config(config, authentication_config)?;

    Ok(())
}

fn append_authentication_config(
    config: &mut BTreeMap<String, String>,
    authentication_config: &Vec<AirflowAuthenticationConfigResolved>,
) -> Result<()> {
    // TODO: we make sure in crd/src/authentication.rs that currently there is only one
    //    AuthenticationClass provided. If the FlaskAppBuilder ever supports this we have
    //    to adapt the config here accordingly
    for auth_config in authentication_config {
        if let Some(auth_class) = &auth_config.authentication_class {
            if let AuthenticationClassProvider::Ldap(ldap) = &auth_class.spec.provider {
                append_ldap_config(config, ldap)?;
            }
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
    }
    Ok(())
}

fn append_ldap_config(
    config: &mut BTreeMap<String, String>,
    ldap: &AuthenticationProvider,
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

#[cfg(test)]
mod tests {
    use crate::config::add_airflow_config;
    use stackable_airflow_crd::authentication::{
        default_sync_roles_at, default_user_registration, AirflowAuthenticationConfigResolved,
    };
    use stackable_airflow_crd::AirflowConfigOptions;
    use stackable_operator::commons::authentication::AuthenticationClass;
    use std::collections::BTreeMap;

    #[test]
    fn test_no_ldap() {
        let mut result = BTreeMap::new();
        add_airflow_config(&mut result, &vec![]).expect("Ok");
        assert_eq!(
            BTreeMap::from([("AUTH_TYPE".into(), "AUTH_DB".into())]),
            result
        );
    }

    #[test]
    fn test_ldap() {
        let authentication_class = "
            apiVersion: authentication.stackable.tech/v1alpha1
            kind: AuthenticationClass
            metadata:
              name: airflow-with-ldap-server-veri-tls-ldap
            spec:
              provider:
                ldap:
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
          ";
        let deserializer = serde_yaml::Deserializer::from_str(authentication_class);
        let authentication_class: AuthenticationClass =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();

        let resolved_config = AirflowAuthenticationConfigResolved {
            authentication_class: Some(authentication_class),
            user_registration: default_user_registration(),
            user_registration_role: "Admin".to_string(),
            sync_roles_at: default_sync_roles_at(),
        };

        let mut result = BTreeMap::new();
        add_airflow_config(&mut result, &vec![resolved_config]).expect("Ok");

        assert_eq!(
            "AUTH_LDAP",
            result
                .get(&AirflowConfigOptions::AuthType.to_string())
                .unwrap()
        );
    }
}
