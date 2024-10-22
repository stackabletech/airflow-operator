mod airflow_controller;
mod config;
mod controller_commons;
mod env_vars;
mod operations;
mod product_logging;
mod util;

use crate::airflow_controller::AIRFLOW_CONTROLLER_NAME;

use clap::{crate_description, crate_version, Parser};
use futures::StreamExt;
use stackable_airflow_crd::{AirflowCluster, APP_NAME, OPERATOR_NAME};
use stackable_operator::{
    cli::{Command, ProductOperatorRun},
    commons::authentication::AuthenticationClass,
    k8s_openapi::api::{apps::v1::StatefulSet, core::v1::Service},
    kube::{
        core::DeserializeGuard,
        runtime::{reflector::ObjectRef, watcher, Controller},
    },
    logging::controller::report_controller_reconciled,
    CustomResourceExt,
};
use std::sync::Arc;

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

#[derive(Parser)]
#[clap(about, author)]
struct Opts {
    #[clap(subcommand)]
    cmd: Command,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();

    match opts.cmd {
        Command::Crd => {
            AirflowCluster::print_yaml_schema(built_info::PKG_VERSION)?;
        }
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
                crate_description!(),
                crate_version!(),
                built_info::GIT_VERSION,
                built_info::TARGET,
                built_info::BUILT_TIME_UTC,
                built_info::RUSTC_VERSION,
            );
            let product_config = product_config.load(&[
                "deploy/config-spec/properties.yaml",
                "/etc/stackable/airflow-operator/config-spec/properties.yaml",
            ])?;

            let client =
                stackable_operator::client::create_client(Some(OPERATOR_NAME.to_string())).await?;

            let airflow_controller_builder = Controller::new(
                watch_namespace.get_api::<DeserializeGuard<AirflowCluster>>(&client),
                watcher::Config::default(),
            );

            let airflow_store_1 = airflow_controller_builder.store();
            let airflow_controller = airflow_controller_builder
                .owns(
                    watch_namespace.get_api::<Service>(&client),
                    watcher::Config::default(),
                )
                .owns(
                    watch_namespace.get_api::<StatefulSet>(&client),
                    watcher::Config::default(),
                )
                .shutdown_on_signal()
                .watches(
                    client.get_api::<DeserializeGuard<AuthenticationClass>>(&()),
                    watcher::Config::default(),
                    move |_| {
                        airflow_store_1
                            .state()
                            .into_iter()
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
                        &format!("{AIRFLOW_CONTROLLER_NAME}.{OPERATOR_NAME}"),
                        &res,
                    );
                });

            airflow_controller.collect::<()>().await;
        }
    }

    Ok(())
}
