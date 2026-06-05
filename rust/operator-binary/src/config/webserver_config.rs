//! Builds the `webserver_config.py` Flask configuration file from the resolved
//! authentication/authorization config plus user-provided config overrides.

use std::{collections::BTreeMap, io::Write};

use snafu::{ResultExt, Snafu};
use stackable_operator::v2::flask_config_writer;

use super::{PYTHON_IMPORTS, add_airflow_config};
use crate::crd::{
    AirflowConfigOptions, authentication::AirflowClientAuthenticationDetailsResolved,
    authorization::AirflowAuthorizationResolved,
};

/// Marks arbitrary Python code to prepend verbatim to the generated file.
const CONFIG_OVERRIDE_FILE_HEADER_KEY: &str = "FILE_HEADER";
/// Marks arbitrary Python code to append verbatim to the generated file.
const CONFIG_OVERRIDE_FILE_FOOTER_KEY: &str = "FILE_FOOTER";

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to construct the webserver config"))]
    ConstructConfig { source: super::Error },

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
    authentication_config: &AirflowClientAuthenticationDetailsResolved,
    authorization_config: &AirflowAuthorizationResolved,
    product_version: &str,
    config_file_overrides: &BTreeMap<String, String>,
) -> Result<String, Error> {
    let mut config: BTreeMap<String, String> = BTreeMap::new();

    // this will call default values from AirflowClientAuthenticationDetails
    add_airflow_config(
        &mut config,
        authentication_config,
        authorization_config,
        product_version,
    )
    .context(ConstructConfigSnafu)?;

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

    flask_config_writer::write::<AirflowConfigOptions, _, _>(
        &mut config_file,
        config.iter(),
        PYTHON_IMPORTS,
    )
    .context(WriteConfigFileSnafu)?;

    if let Some(footer) = temp_file_footer {
        writeln!(config_file, "{}", footer).context(WriteHeaderFooterSnafu)?;
    }

    Ok(String::from_utf8(config_file).expect("the Flask config writer only emits valid UTF-8"))
}
