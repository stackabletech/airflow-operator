//! Ensures that `Pod`s are configured and running for each [`AirflowCluster`]

use futures::{future, StreamExt};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_airflow_crd::{
    commands::{CommandStatus, Init},
    AirflowCluster, AirflowClusterRef,
};
use stackable_operator::{
    builder::{ContainerBuilder, ObjectMetaBuilder},
    k8s_openapi::{
        api::{
            batch::v1::{Job, JobSpec},
            core::v1::{EnvVar, EnvVarSource, PodSpec, PodTemplateSpec, SecretKeySelector},
        },
        apimachinery::pkg::apis::meta::v1::Time,
        chrono::Utc,
    },
    kube::{
        api::ListParams,
        core::DynamicObject,
        runtime::{
            self,
            controller::{Context, ReconcilerAction},
            reflector::ObjectRef,
        },
        ResourceExt,
    },
    logging::controller::ReconcilerError,
};
use std::{sync::Arc, time::Duration};
use strum::{EnumDiscriminants, IntoStaticStr};

const FIELD_MANAGER_SCOPE: &str = "airflowcluster";

pub struct Ctx {
    pub client: stackable_operator::client::Client,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object does not refer to AirflowCluster"))]
    InvalidAirflowReference,
    #[snafu(display("could not find object {airflow}"))]
    FindAirflow {
        source: stackable_operator::error::Error,
        airflow: ObjectRef<AirflowCluster>,
    },
    #[snafu(display("object defines no version"))]
    ObjectHasNoVersion,
    #[snafu(display("failed to apply Job for {airflow}"))]
    ApplyJob {
        source: stackable_operator::error::Error,
        airflow: ObjectRef<AirflowCluster>,
    },
    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
    },
}
type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }

    fn secondary_object(&self) -> Option<ObjectRef<DynamicObject>> {
        match self {
            Error::InvalidAirflowReference => None,
            Error::FindAirflow { airflow, .. } => Some(airflow.clone().erase()),
            Error::ObjectHasNoVersion => None,
            Error::ApplyJob { airflow, .. } => Some(airflow.clone().erase()),
            Error::ApplyStatus { .. } => None,
            Error::ObjectMissingMetadataForOwnerRef { .. } => None,
        }
    }
}

pub async fn reconcile_init(init: Arc<Init>, ctx: Context<Ctx>) -> Result<ReconcilerAction> {
    tracing::info!("Starting reconcile");

    let client = &ctx.get_ref().client;

    let airflow = find_airflow_cluster_of_init_command(client, &init).await?;

    let job = build_init_job(&init, &airflow)?;
    client
        .apply_patch(FIELD_MANAGER_SCOPE, &job, &job)
        .await
        .with_context(|_| ApplyJobSnafu {
            airflow: ObjectRef::from_obj(&airflow),
        })?;

    if init.status == None {
        let started_at = Some(Time(Utc::now()));
        client
            .apply_patch_status(
                FIELD_MANAGER_SCOPE,
                &*init,
                &CommandStatus {
                    started_at: started_at.to_owned(),
                    finished_at: None,
                },
            )
            .await
            .context(ApplyStatusSnafu)?;

        wait_completed(client, &job).await;

        let finished_at = Some(Time(Utc::now()));
        client
            .apply_patch_status(
                FIELD_MANAGER_SCOPE,
                &*init,
                &CommandStatus {
                    started_at,
                    finished_at,
                },
            )
            .await
            .context(ApplyStatusSnafu)?;
    }

    Ok(ReconcilerAction {
        requeue_after: None,
    })
}

fn build_init_job(init: &Init, airflow: &AirflowCluster) -> Result<Job> {
    let commands = vec![
        String::from("airflow db init"),
        String::from("airflow db upgrade"),
        String::from(
            "airflow users create \
                    --username \"$ADMIN_USERNAME\" \
                    --firstname \"$ADMIN_FIRSTNAME\" \
                    --lastname \"$ADMIN_LASTNAME\" \
                    --email \"$ADMIN_EMAIL\" \
                    --password \"$ADMIN_PASSWORD\" \
                    --role \"Admin\"",
        ),
    ];

    let version = airflow_version(airflow)?;
    tracing::info!("version {:?}", version);
    let secret = &init.spec.credentials_secret;

    let env = vec![
        env_var_from_secret("SECRET_KEY", secret, "connections.secretKey"),
        env_var_from_secret(
            "AIRFLOW__CORE__SQL_ALCHEMY_CONN",
            secret,
            "connections.sqlalchemyDatabaseUri",
        ),
        env_var_from_secret(
            "AIRFLOW__CELERY__RESULT_BACKEND",
            secret,
            "connections.celeryResultBackend",
        ),
        env_var_from_secret("ADMIN_USERNAME", secret, "adminUser.username"),
        env_var_from_secret("ADMIN_FIRSTNAME", secret, "adminUser.firstname"),
        env_var_from_secret("ADMIN_LASTNAME", secret, "adminUser.lastname"),
        env_var_from_secret("ADMIN_EMAIL", secret, "adminUser.email"),
        env_var_from_secret("ADMIN_PASSWORD", secret, "adminUser.password"),
    ];

    let container = ContainerBuilder::new("airflow-init")
        .image(format!(
            "docker.stackable.tech/stackable/airflow:{}-stackable0",
            version
        ))
        .command(vec!["/bin/bash".to_string()])
        .args(vec![String::from("-c"), commands.join("; ")])
        .add_env_vars(env)
        .build();

    let pod = PodTemplateSpec {
        metadata: Some(
            ObjectMetaBuilder::new()
                .name(format!("{}-init", init.name()))
                .build(),
        ),
        spec: Some(PodSpec {
            containers: vec![container],
            restart_policy: Some("Never".to_string()),
            ..Default::default()
        }),
    };

    let job = Job {
        metadata: ObjectMetaBuilder::new()
            .name(init.name())
            .namespace_opt(init.namespace())
            .ownerreference_from_resource(init, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .build(),
        spec: Some(JobSpec {
            template: pod,
            ..Default::default()
        }),
        status: None,
    };

    Ok(job)
}

async fn find_airflow_cluster_of_init_command(
    client: &stackable_operator::client::Client,
    init: &Init,
) -> Result<AirflowCluster, Error> {
    if let AirflowClusterRef {
        name: Some(airflow_name),
        namespace: maybe_airflow_ns,
    } = &init.spec.cluster_ref
    {
        let init_ns = init.namespace().unwrap_or_else(|| "default".to_string());
        let airflow_ns = maybe_airflow_ns.as_deref().unwrap_or(init_ns.as_str());
        client
            .get::<AirflowCluster>(airflow_name, Some(airflow_ns))
            .await
            .context(FindAirflowSnafu {
                airflow: ObjectRef::new(airflow_name).within(airflow_ns),
            })
    } else {
        InvalidAirflowReferenceSnafu.fail()
    }
}

// Waits until the given job is completed.
async fn wait_completed(client: &stackable_operator::client::Client, job: &Job) {
    let completed = |job: &Job| {
        job.status
            .as_ref()
            .and_then(|status| status.conditions.clone())
            .unwrap_or_default()
            .into_iter()
            .any(|condition| condition.type_ == "Complete" && condition.status == "True")
    };

    let lp = ListParams::default().fields(&format!("metadata.name={}", job.name()));
    let api = client.get_api(Some(job.namespace().as_deref().unwrap_or("default")));
    let watcher = runtime::watcher(api, lp).boxed();
    runtime::utils::try_flatten_applied(watcher)
        .any(|res| future::ready(res.as_ref().map(|job| completed(job)).unwrap_or(false)))
        .await;
}

pub fn airflow_version(airflow: &AirflowCluster) -> Result<&str> {
    airflow
        .spec
        .version
        .as_deref()
        .context(ObjectHasNoVersionSnafu)
}

pub fn error_policy(_error: &Error, _ctx: Context<Ctx>) -> ReconcilerAction {
    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(5)),
    }
}

fn env_var_from_secret(var_name: &str, secret: &str, secret_key: &str) -> EnvVar {
    EnvVar {
        name: String::from(var_name),
        value_from: Some(EnvVarSource {
            secret_key_ref: Some(SecretKeySelector {
                name: Some(String::from(secret)),
                key: String::from(secret_key),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}
