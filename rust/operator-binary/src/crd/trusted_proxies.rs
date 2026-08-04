use std::{fmt::Display, net::IpAddr, str::FromStr};

use snafu::{OptionExt, Snafu, ensure};

/// Trusts every peer, regardless of its address.
const WILDCARD: &str = "*";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "the trusted proxy {value:?} is neither an IP address, a CIDR network, nor {WILDCARD:?}"
    ))]
    InvalidIpAddress { value: String },

    #[snafu(display("the trusted proxy {value:?} has a prefix length that is not a number"))]
    InvalidPrefixLength { value: String },

    #[snafu(display(
        "the trusted proxy {value:?} has a prefix length of {prefix_length}, which exceeds the \
         maximum of {maximum} for its address family"
    ))]
    PrefixLengthOutOfRange {
        value: String,
        prefix_length: u8,
        maximum: u8,
    },
}

/// A single entry of the trusted-proxy list: an IP address (`10.0.0.1`), a CIDR network
/// (`10.244.0.0/16`), or `*` for every peer.
///
/// The value is handed to Airflow verbatim, which is why it is kept as a string rather than a
/// parsed network: uvicorn accepts all three notations, and round-tripping through an
/// `IpAddr`/network type would normalise the user's input for no benefit.
///
/// Parsing still happens up front: an entry uvicorn cannot parse is silently treated as
/// an opaque literal that matches no peer, which disables proxy trust without any error. Failing
/// reconciliation instead makes the misconfiguration visible.
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(dead_code)]
pub struct TrustedProxy(String);

impl FromStr for TrustedProxy {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value == WILDCARD {
            return Ok(Self(value.to_owned()));
        }

        // Split at the *last* `/` so that a doubled prefix like `10.0.0.0/16/24` leaves
        // `10.0.0.0/16` as the address part and is rejected as an address rather than as a
        // prefix length, which is the clearer of the two messages.
        let (address, prefix_length) = match value.rsplit_once('/') {
            Some((address, prefix_length)) => (address, Some(prefix_length)),
            None => (value, None),
        };

        let address = address
            .parse::<IpAddr>()
            .ok()
            .context(InvalidIpAddressSnafu { value })?;

        if let Some(prefix_length) = prefix_length {
            let prefix_length = prefix_length
                .parse::<u8>()
                .ok()
                .context(InvalidPrefixLengthSnafu { value })?;

            let maximum = match address {
                IpAddr::V4(_) => 32,
                IpAddr::V6(_) => 128,
            };

            ensure!(
                prefix_length <= maximum,
                PrefixLengthOutOfRangeSnafu {
                    value,
                    prefix_length,
                    maximum,
                }
            );
        }

        Ok(Self(value.to_owned()))
    }
}

impl Display for TrustedProxy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("10.244.0.0/16")]
    #[case("192.168.1.1")]
    #[case("::1")]
    #[case("fd00::/8")]
    #[case("0.0.0.0/0")]
    #[case("*")]
    fn accepts_addresses_networks_and_wildcard(#[case] value: &str) {
        let proxy = TrustedProxy::from_str(value).expect("must be accepted");
        // The string form is what is handed to Airflow, so it must survive verbatim.
        assert_eq!(proxy.to_string(), value);
    }

    #[rstest]
    #[case("")]
    #[case("not-an-ip")]
    #[case("10.244.0.0/16/24")]
    #[case("airflow.example.com")]
    fn rejects_values_that_are_not_addresses(#[case] value: &str) {
        assert!(matches!(
            TrustedProxy::from_str(value),
            Err(Error::InvalidIpAddress { .. })
        ));
    }

    #[test]
    fn rejects_a_non_numeric_prefix_length() {
        assert!(matches!(
            TrustedProxy::from_str("10.244.0.0/sixteen"),
            Err(Error::InvalidPrefixLength { .. })
        ));
    }

    #[rstest]
    #[case("10.244.0.0/33", 33, 32)]
    #[case("fd00::/129", 129, 128)]
    fn rejects_a_prefix_length_beyond_the_address_family_maximum(
        #[case] value: &str,
        #[case] expected_prefix_length: u8,
        #[case] expected_maximum: u8,
    ) {
        let error = TrustedProxy::from_str(value).expect_err("must be rejected");
        assert!(matches!(
            error,
            Error::PrefixLengthOutOfRange {
                prefix_length,
                maximum,
                ..
            } if prefix_length == expected_prefix_length && maximum == expected_maximum
        ));
    }

    /// The rendered message must name the offending value, because it is the only thing that tells
    /// a user which of their list entries is wrong.
    #[test]
    fn error_message_names_the_offending_value() {
        let error = TrustedProxy::from_str("not-an-ip").expect_err("must be rejected");
        assert!(
            error.to_string().contains("not-an-ip"),
            "message was: {error}"
        );
    }
}
