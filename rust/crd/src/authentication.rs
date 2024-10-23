use std::{future::Future, mem};

use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    commons::authentication::{
        ldap,
        oidc::{self, IdentityProviderHint},
        AuthenticationClass, AuthenticationClassProvider, ClientAuthenticationDetails,
    },
    schemars::{self, JsonSchema},
};
use std::collections::BTreeSet;
use tracing::info;

const SUPPORTED_AUTHENTICATION_CLASS_PROVIDERS: [&str; 2] = ["LDAP", "OIDC"];
const SUPPORTED_OIDC_PROVIDERS: &[oidc::IdentityProviderHint] =
    &[oidc::IdentityProviderHint::Keycloak];
// The assumed OIDC provider if no hint is given in the AuthClass
pub const DEFAULT_OIDC_PROVIDER: oidc::IdentityProviderHint = oidc::IdentityProviderHint::Keycloak;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display(
        "The AuthenticationClass {auth_class_name:?} is referenced several times which is not allowed."
    ))]
    DuplicateAuthenticationClassReferencesNotAllowed { auth_class_name: String },

    #[snafu(display("Failed to retrieve AuthenticationClass"))]
    AuthenticationClassRetrievalFailed {
        source: stackable_operator::client::Error,
    },
    // TODO: Adapt message if multiple authentication classes are supported simultaneously
    #[snafu(display("Only one authentication class is currently supported at a time"))]
    MultipleAuthenticationClassesProvided,
    #[snafu(display(
        "Failed to use authentication provider [{provider}] for authentication class [{auth_class_name}] - supported providers: {SUPPORTED_AUTHENTICATION_CLASS_PROVIDERS:?}",
    ))]
    AuthenticationProviderNotSupported {
        auth_class_name: String,
        provider: String,
    },
    #[snafu(display("Only one authentication type at a time is supported by Airflow, see https://github.com/dpgaspar/Flask-AppBuilder/issues/1924."))]
    MultipleAuthenticationTypesNotSupported,
    #[snafu(display("Only one LDAP provider at a time is supported by Airflow."))]
    MultipleLdapProvidersNotSupported,
    #[snafu(display("The OIDC provider {oidc_provider:?} is not yet supported (AuthenticationClass {auth_class_name:?})."))]
    OidcProviderNotSupported {
        auth_class_name: String,
        oidc_provider: String,
    },
    #[snafu(display(
        "TLS verification cannot be disabled in Superset (AuthenticationClass {auth_class_name:?})."
    ))]
    TlsVerificationCannotBeDisabled { auth_class_name: String },
    #[snafu(display(
        "The userRegistrationRole settings must not differ between the authentication entries.",
    ))]
    DifferentUserRegistrationRoleSettingsNotAllowed,
    #[snafu(display(
        "The userRegistration settings must not differ between the authentication entries.",
    ))]
    DifferentUserRegistrationSettingsNotAllowed,
    #[snafu(display(
        "The syncRolesAt settings must not differ between the authentication entries.",
    ))]
    DifferentSyncRolesAtSettingsNotAllowed,
    #[snafu(display("Invalid OIDC configuration"))]
    OidcConfigurationInvalid {
        source: stackable_operator::commons::authentication::Error,
    },
    #[snafu(display(
        "{configured:?} is not a supported principalClaim in Airflow for the Keycloak OIDC provider. Please use {supported:?} in the AuthenticationClass {auth_class_name:?}"
    ))]
    OidcPrincipalClaimNotSupported {
        configured: String,
        supported: String,
        auth_class_name: String,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AirflowClientAuthenticationDetails {
    #[serde(flatten)]
    pub common: ClientAuthenticationDetails<()>,

    /// Allow users who are not already in the FAB DB.
    /// Gets mapped to `AUTH_USER_REGISTRATION`
    #[serde(default = "default_user_registration")]
    pub user_registration: bool,

    /// This role will be given in addition to any AUTH_ROLES_MAPPING.
    /// Gets mapped to `AUTH_USER_REGISTRATION_ROLE`
    #[serde(default = "default_user_registration_role")]
    pub user_registration_role: String,

    /// If we should replace ALL the user's roles each login, or only on registration.
    /// Gets mapped to `AUTH_ROLES_SYNC_AT_LOGIN`
    #[serde(default)]
    pub sync_roles_at: FlaskRolesSyncMoment,
}

pub fn default_user_registration() -> bool {
    true
}

pub fn default_user_registration_role() -> String {
    "Public".to_string()
}

/// Matches Flask's default mode of syncing at registration
pub fn default_sync_roles_at() -> FlaskRolesSyncMoment {
    FlaskRolesSyncMoment::Registration
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize, Default)]
pub enum FlaskRolesSyncMoment {
    #[default]
    Registration,
    Login,
}

/// Resolved and validated counter part for `AirflowClientAuthenticationDetails`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AirflowClientAuthenticationDetailsResolved {
    pub authentication_classes_resolved: Vec<AirflowAuthenticationClassResolved>,
    pub user_registration: bool,
    pub user_registration_role: String,
    pub sync_roles_at: FlaskRolesSyncMoment,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AirflowAuthenticationClassResolved {
    Ldap {
        provider: ldap::AuthenticationProvider,
    },
    Oidc {
        provider: oidc::AuthenticationProvider,
        oidc: oidc::ClientAuthenticationOptions<()>,
    },
}

impl AirflowClientAuthenticationDetailsResolved {
    pub async fn from(
        auth_details: &[AirflowClientAuthenticationDetails],
        client: &Client,
    ) -> Result<AirflowClientAuthenticationDetailsResolved> {
        let resolve_auth_class = |auth_details: ClientAuthenticationDetails| async move {
            auth_details.resolve_class(client).await
        };
        AirflowClientAuthenticationDetailsResolved::resolve(auth_details, resolve_auth_class).await
    }
    pub async fn resolve<R>(
        auth_details: &[AirflowClientAuthenticationDetails],
        resolve_auth_class: impl Fn(ClientAuthenticationDetails) -> R,
    ) -> Result<AirflowClientAuthenticationDetailsResolved>
    where
        R: Future<Output = Result<AuthenticationClass, stackable_operator::client::Error>>,
    {
        let mut resolved_auth_classes: Vec<AirflowAuthenticationClassResolved> = Vec::new();
        let mut user_registration = None;
        let mut user_registration_role = None;
        let mut sync_roles_at = None;

        let mut auth_class_names = BTreeSet::new();

        for entry in auth_details {
            let auth_class_name = entry.common.authentication_class_name();

            let is_new_auth_class = auth_class_names.insert(auth_class_name);
            ensure!(
                is_new_auth_class,
                DuplicateAuthenticationClassReferencesNotAllowedSnafu { auth_class_name }
            );

            let auth_class = resolve_auth_class(entry.common.clone())
                .await
                .context(AuthenticationClassRetrievalFailedSnafu)?;

            match &auth_class.spec.provider {
                AuthenticationClassProvider::Ldap(provider) => {
                    let resolved_auth_class = AirflowAuthenticationClassResolved::Ldap {
                        provider: provider.to_owned(),
                    };
                    if let Some(other) = resolved_auth_classes.first() {
                        ensure!(
                            mem::discriminant(other) == mem::discriminant(&resolved_auth_class),
                            MultipleAuthenticationTypesNotSupportedSnafu
                        );
                    }

                    ensure!(
                        resolved_auth_classes.is_empty(),
                        MultipleLdapProvidersNotSupportedSnafu
                    );

                    resolved_auth_classes.push(resolved_auth_class);
                }
                AuthenticationClassProvider::Oidc(provider) => {
                    let resolved_auth_class =
                        AirflowClientAuthenticationDetailsResolved::from_oidc(
                            auth_class_name,
                            provider,
                            entry,
                        )?;

                    if let Some(other) = resolved_auth_classes.first() {
                        ensure!(
                            mem::discriminant(other) == mem::discriminant(&resolved_auth_class),
                            MultipleAuthenticationTypesNotSupportedSnafu
                        );
                    }
                    resolved_auth_classes.push(resolved_auth_class);
                }

                _ => {
                    return Err(Error::AuthenticationProviderNotSupported {
                        auth_class_name: auth_class_name.to_owned(),
                        provider: auth_class.spec.provider.to_string(),
                    });
                }
            }

            match user_registration {
                Some(user_registration) => {
                    ensure!(
                        user_registration == entry.user_registration,
                        DifferentUserRegistrationSettingsNotAllowedSnafu
                    );
                }
                None => user_registration = Some(entry.user_registration),
            }
            match &user_registration_role {
                Some(user_registration_role) => {
                    ensure!(
                        user_registration_role == &entry.user_registration_role,
                        DifferentUserRegistrationRoleSettingsNotAllowedSnafu
                    );
                }
                None => user_registration_role = Some(entry.user_registration_role.to_owned()),
            }
            match &sync_roles_at {
                Some(sync_roles_at) => {
                    ensure!(
                        sync_roles_at == &entry.sync_roles_at,
                        DifferentSyncRolesAtSettingsNotAllowedSnafu
                    );
                }
                None => sync_roles_at = Some(entry.sync_roles_at.to_owned()),
            }
        }
        Ok(AirflowClientAuthenticationDetailsResolved {
            authentication_classes_resolved: resolved_auth_classes,
            user_registration: user_registration.unwrap_or_else(default_user_registration),
            user_registration_role: user_registration_role
                .unwrap_or_else(default_user_registration_role),
            sync_roles_at: sync_roles_at.unwrap_or_else(FlaskRolesSyncMoment::default),
        })
    }

    fn from_oidc(
        auth_class_name: &str,
        provider: &oidc::AuthenticationProvider,
        auth_details: &AirflowClientAuthenticationDetails,
    ) -> Result<AirflowAuthenticationClassResolved> {
        let oidc_provider = match &provider.provider_hint {
            None => {
                info!("No OIDC provider hint given in AuthClass {auth_class_name}, assuming {default_oidc_provider_name}",
                default_oidc_provider_name = serde_json::to_string(&DEFAULT_OIDC_PROVIDER).unwrap());
                DEFAULT_OIDC_PROVIDER
            }
            Some(oidc_provider) => oidc_provider.to_owned(),
        };

        ensure!(
            SUPPORTED_OIDC_PROVIDERS.contains(&oidc_provider),
            OidcProviderNotSupportedSnafu {
                auth_class_name,
                oidc_provider: serde_json::to_string(&oidc_provider).unwrap(),
            }
        );

        match oidc_provider {
            IdentityProviderHint::Keycloak => {
                ensure!(
                    &provider.principal_claim == "preferred_username",
                    OidcPrincipalClaimNotSupportedSnafu {
                        configured: provider.principal_claim.clone(),
                        supported: "preferred_username".to_owned(),
                        auth_class_name,
                    }
                );
            }
        }

        ensure!(
            !provider.tls.uses_tls() || provider.tls.uses_tls_verification(),
            TlsVerificationCannotBeDisabledSnafu { auth_class_name }
        );

        Ok(AirflowAuthenticationClassResolved::Oidc {
            provider: provider.to_owned(),
            oidc: auth_details
                .common
                .oidc_or_error(auth_class_name)
                .context(OidcConfigurationInvalidSnafu)?
                .clone(),
        })
    }
}
