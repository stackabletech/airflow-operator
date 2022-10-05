mod airflow_controller;
mod airflow_db_controller;
mod config;
mod rbac;
mod util;

use std::sync::Arc;

use clap::Parser;
use futures::StreamExt;
use stackable_airflow_crd::{
    airflowdb::AirflowDB, AirflowCluster, AirflowClusterAuthenticationConfig, APP_NAME,
};
use stackable_operator::cli::ProductOperatorRun;
use stackable_operator::logging::controller::report_controller_reconciled;
use stackable_operator::{
    cli::Command,
    commons::authentication::AuthenticationClass,
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        batch::v1::Job,
        core::v1::{Secret, Service},
    },
    kube::{
        api::ListParams,
        runtime::{reflector::ObjectRef, Controller},
        CustomResourceExt, ResourceExt,
    },
};

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

#[derive(Parser)]
#[clap(about = built_info::PKG_DESCRIPTION, author = stackable_operator::cli::AUTHOR)]
struct Opts {
    #[clap(subcommand)]
    cmd: Command,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();

    match opts.cmd {
        Command::Crd => println!(
            "{}{}",
            serde_yaml::to_string(&AirflowCluster::crd())?,
            serde_yaml::to_string(&AirflowDB::crd())?
        ),
        Command::Run(ProductOperatorRun {
            product_config,
            watch_namespace,
            tracing_target,
        }) => {
            stackable_operator::logging::initialize_logging(
                "AIRFLOW_OPERATOR_LOG",
                APP_NAME,
                tracing_target,
            );
            stackable_operator::utils::print_startup_string(
                built_info::PKG_DESCRIPTION,
                built_info::PKG_VERSION,
                built_info::GIT_VERSION,
                built_info::TARGET,
                built_info::BUILT_TIME_UTC,
                built_info::RUSTC_VERSION,
            );
            let product_config = product_config.load(&[
                "deploy/config-spec/properties.yaml",
                "/etc/stackable/airflow-operator/config-spec/properties.yaml",
            ])?;

            let client = stackable_operator::client::create_client(Some(
                "airflow.stackable.tech".to_string(),
            ))
            .await?;

            let airflow_controller_builder = Controller::new(
                watch_namespace.get_api::<AirflowCluster>(&client),
                ListParams::default(),
            );

            let airflow_store_1 = airflow_controller_builder.store();
            let airflow_store_2 = airflow_controller_builder.store();
            let airflow_controller = airflow_controller_builder
                .owns(
                    watch_namespace.get_api::<Service>(&client),
                    ListParams::default(),
                )
                .owns(
                    watch_namespace.get_api::<StatefulSet>(&client),
                    ListParams::default(),
                )
                .shutdown_on_signal()
                .watches(
                    watch_namespace.get_api::<AuthenticationClass>(&client),
                    ListParams::default(),
                    move |authentication_class| {
                        airflow_store_1
                            .state()
                            .into_iter()
                            .filter(move |airflow: &Arc<AirflowCluster>| {
                                references_authentication_class(
                                    &airflow.spec.authentication_config,
                                    &authentication_class,
                                )
                            })
                            .map(|airflow| ObjectRef::from_obj(&*airflow))
                    },
                )
                .watches(
                    watch_namespace.get_api::<AirflowDB>(&client),
                    ListParams::default(),
                    move |airflow_db| {
                        airflow_store_2
                            .state()
                            .into_iter()
                            .filter(move |airflow| {
                                airflow_db.name_unchecked() == airflow.name_unchecked()
                                    && airflow_db.namespace() == airflow.namespace()
                            })
                            .map(|airflow| ObjectRef::from_obj(&*airflow))
                    },
                )
                .run(
                    airflow_controller::reconcile_airflow,
                    airflow_controller::error_policy,
                    Arc::new(airflow_controller::Ctx {
                        client: client.clone(),
                        product_config,
                    }),
                )
                .map(|res| {
                    report_controller_reconciled(
                        &client,
                        "airflowclusters.airflow.stackable.tech",
                        &res,
                    );
                });

            let airflow_db_controller_builder = Controller::new(
                watch_namespace.get_api::<AirflowDB>(&client),
                ListParams::default(),
            );

            let airflow_db_store1 = airflow_db_controller_builder.store();
            let airflow_db_store2 = airflow_db_controller_builder.store();
            let airflow_db_controller = airflow_db_controller_builder
                .shutdown_on_signal()
                .watches(
                    watch_namespace.get_api::<Secret>(&client),
                    ListParams::default(),
                    move |secret| {
                        airflow_db_store1
                            .state()
                            .into_iter()
                            .filter(move |airflow_db| {
                                if let Some(n) = &secret.metadata.name {
                                    &airflow_db.spec.credentials_secret == n
                                } else {
                                    false
                                }
                            })
                            .map(|airflow_db| ObjectRef::from_obj(&*airflow_db))
                    },
                )
                // We have to watch jobs so we can react to finished init jobs
                // and update our status accordingly
                .watches(
                    watch_namespace.get_api::<Job>(&client),
                    ListParams::default(),
                    move |job| {
                        airflow_db_store2
                            .state()
                            .into_iter()
                            .filter(move |airflow_db| {
                                job.name_unchecked() == airflow_db.name_unchecked()
                                    && job.namespace() == airflow_db.namespace()
                            })
                            .map(|airflow_db| ObjectRef::from_obj(&*airflow_db))
                    },
                )
                .run(
                    airflow_db_controller::reconcile_airflow_db,
                    airflow_db_controller::error_policy,
                    Arc::new(airflow_db_controller::Ctx {
                        client: client.clone(),
                    }),
                )
                .map(|res| {
                    report_controller_reconciled(
                        &client,
                        "airflowdbclusters.airflow.stackable.tech",
                        &res,
                    )
                });

            futures::stream::select(airflow_controller, airflow_db_controller)
                .collect::<()>()
                .await;
        }
    }

    Ok(())
}

fn references_authentication_class(
    authentication_config: &Option<AirflowClusterAuthenticationConfig>,
    authentication_class: &AuthenticationClass,
) -> bool {
    assert!(authentication_class.metadata.name.is_some());

    authentication_config
        .as_ref()
        .and_then(|c| c.authentication_class.as_ref())
        == authentication_class.metadata.name.as_ref()
}
