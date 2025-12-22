use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    kube::{Client, core::crd::MergeError},
    webhook::{
        maintainer::CustomResourceDefinitionMaintainer,
        servers::{ConversionWebhookError, ConversionWebhookServer},
    },
};
use tokio::sync::oneshot;

use crate::crd::{AirflowCluster, AirflowClusterVersion, FIELD_MANAGER};

/// Contains errors which can be encountered when creating the conversion webhook server and the
/// CRD maintainer.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("failed to merge CRD"))]
    MergeCrd { source: MergeError },

    #[snafu(display("failed to create conversion webhook server"))]
    CreateConversionWebhook { source: ConversionWebhookError },
}

/// Creates and returns a [`ConversionWebhookServer`] and a [`CustomResourceDefinitionMaintainer`].
pub async fn create_webhook_and_maintainer<'a>(
    operator_environment: &'a OperatorEnvironmentOptions,
    disable_crd_maintenance: bool,
    client: Client,
) -> Result<
    (
        ConversionWebhookServer,
        CustomResourceDefinitionMaintainer<'a>,
        oneshot::Receiver<()>,
    ),
    Error,
> {
    let crds_and_handlers = [(
        AirflowCluster::merged_crd(AirflowClusterVersion::V1Alpha2).context(MergeCrdSnafu)?,
        AirflowCluster::try_convert as fn(_) -> _,
    )];

    ConversionWebhookServer::with_maintainer(
        crds_and_handlers,
        &operator_environment.operator_service_name,
        &operator_environment.operator_namespace,
        FIELD_MANAGER,
        disable_crd_maintenance,
        client,
    )
    .await
    .context(CreateConversionWebhookSnafu)
}
