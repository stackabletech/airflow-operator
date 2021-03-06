use crate::util::{env_var_from_secret, get_job_state, JobState};

use snafu::{ResultExt, Snafu};
use stackable_airflow_crd::airflowdb::{AirflowDB, AirflowDBStatus, AirflowDBStatusCondition};
use stackable_airflow_crd::AirflowCluster;
use stackable_operator::{
    builder::{ContainerBuilder, ObjectMetaBuilder},
    k8s_openapi::api::{
        batch::v1::{Job, JobSpec},
        core::v1::{PodSpec, PodTemplateSpec, Secret},
    },
    kube::{
        runtime::{controller::Action, reflector::ObjectRef},
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
    #[snafu(display("failed to apply Job for {}", airflow_db))]
    ApplyJob {
        source: stackable_operator::error::Error,
        airflow_db: ObjectRef<AirflowDB>,
    },
    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("database state is 'initializing' but failed to find job {}", init_job))]
    GetInitializationJob {
        source: stackable_operator::error::Error,
        init_job: ObjectRef<Job>,
    },
    #[snafu(display("Failed to check whether the secret ({}) exists", secret))]
    SecretCheck {
        source: stackable_operator::error::Error,
        secret: ObjectRef<Secret>,
    },
}
type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

pub async fn reconcile_airflow_db(airflow_db: Arc<AirflowDB>, ctx: Arc<Ctx>) -> Result<Action> {
    tracing::info!("Starting reconcile");

    let client = &ctx.client;

    if let Some(ref s) = airflow_db.status {
        match s.condition {
            AirflowDBStatusCondition::Pending => {
                let secret_exists = client
                    .exists::<Secret>(
                        &airflow_db.spec.credentials_secret,
                        airflow_db.namespace().as_deref(),
                    )
                    .await
                    .with_context(|_| {
                        let mut secret_ref =
                            ObjectRef::<Secret>::new(&airflow_db.spec.credentials_secret);
                        if let Some(ns) = airflow_db.namespace() {
                            secret_ref = secret_ref.within(&ns);
                        }
                        SecretCheckSnafu { secret: secret_ref }
                    })?;
                if secret_exists {
                    let job = build_init_job(&airflow_db)?;
                    client
                        .apply_patch(FIELD_MANAGER_SCOPE, &job, &job)
                        .await
                        .context(ApplyJobSnafu {
                            airflow_db: ObjectRef::from_obj(&*airflow_db),
                        })?;
                    // The job is started, update status to reflect new state
                    client
                        .apply_patch_status(FIELD_MANAGER_SCOPE, &*airflow_db, &s.initializing())
                        .await
                        .context(ApplyStatusSnafu)?;
                }
            }
            AirflowDBStatusCondition::Initializing => {
                // In here, check the associated job that is running.
                // If it is still running, do nothing. If it completed, set status to ready, if it failed, set status to failed.
                let ns = airflow_db
                    .namespace()
                    .unwrap_or_else(|| "default".to_string());
                let job_name = airflow_db.job_name();
                let job = client.get::<Job>(&job_name, Some(&ns)).await.context(
                    GetInitializationJobSnafu {
                        init_job: ObjectRef::<Job>::new(&job_name).within(&ns),
                    },
                )?;

                let new_status = match get_job_state(&job) {
                    JobState::Complete => Some(s.ready()),
                    JobState::Failed => Some(s.failed()),
                    JobState::InProgress => None,
                };

                if let Some(ns) = new_status {
                    client
                        .apply_patch_status(FIELD_MANAGER_SCOPE, &*airflow_db, &ns)
                        .await
                        .context(ApplyStatusSnafu)?;
                }
            }
            AirflowDBStatusCondition::Ready => (),
            AirflowDBStatusCondition::Failed => (),
        }
    } else {
        // Status is none => initialize the status object as "Provisioned"
        let new_status = AirflowDBStatus::new();
        client
            .apply_patch_status(FIELD_MANAGER_SCOPE, &*airflow_db, &new_status)
            .await
            .context(ApplyStatusSnafu)?;
    }

    Ok(Action::await_change())
}

fn build_init_job(airflow_db: &AirflowDB) -> Result<Job> {
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

    let secret = &airflow_db.spec.credentials_secret;

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

    let container = ContainerBuilder::new("airflow-init-db")
        .image(format!(
            "docker.stackable.tech/stackable/airflow:{}",
            airflow_db.spec.airflow_version
        ))
        .command(vec!["/bin/bash".to_string()])
        .args(vec![String::from("-c"), commands.join("; ")])
        .add_env_vars(env)
        .build();

    let pod = PodTemplateSpec {
        metadata: Some(
            ObjectMetaBuilder::new()
                .name(format!("{}-init", airflow_db.name()))
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
            .name(airflow_db.name())
            .namespace_opt(airflow_db.namespace())
            .ownerreference_from_resource(airflow_db, None, Some(true))
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

pub fn error_policy(_error: &Error, _ctx: Arc<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(5))
}
