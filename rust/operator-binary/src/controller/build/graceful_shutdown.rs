use snafu::{ResultExt, Snafu};
use stackable_operator::{builder::pod::PodBuilder, shared::time::Duration};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to set terminationGracePeriod"))]
    SetTerminationGracePeriod {
        source: stackable_operator::builder::pod::Error,
    },
}

/// Sets the Pod's `terminationGracePeriod` from the merged config's graceful-shutdown timeout.
///
/// The timeout is always present (the merge mechanism provides a default, so users cannot disable
/// graceful shutdown); `None` is therefore a no-op.
pub fn add_graceful_shutdown_config(
    graceful_shutdown_timeout: Option<Duration>,
    pod_builder: &mut PodBuilder,
) -> Result<(), Error> {
    if let Some(graceful_shutdown_timeout) = graceful_shutdown_timeout {
        pod_builder
            .termination_grace_period(&graceful_shutdown_timeout)
            .context(SetTerminationGracePeriodSnafu)?;
    }

    Ok(())
}
