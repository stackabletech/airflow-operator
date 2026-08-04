use std::{
    fmt::Display,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    num::ParseIntError,
    str::FromStr,
};

use snafu::{ResultExt, Snafu, ensure};

/// Trusts every peer, regardless of its address.
const WILDCARD: &str = "*";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "the trusted proxy {value:?} is neither an IP address, a CIDR network, nor {WILDCARD:?}"
    ))]
    InvalidIpAddress {
        value: String,
        source: std::net::AddrParseError,
    },

    #[snafu(display(
        "the trusted proxy {value:?} has a prefix length that is not a whole number between 0 \
         and the address family's maximum"
    ))]
    InvalidPrefixLength {
        value: String,
        source: ParseIntError,
    },

    #[snafu(display(
        "the trusted proxy {value:?} has a prefix length of {prefix_length}, which exceeds the \
         maximum of {maximum} for its address family"
    ))]
    PrefixLengthOutOfRange {
        value: String,
        prefix_length: u8,
        maximum: u8,
    },

    #[snafu(display("the trusted proxy {value:?} has host bits set; did you mean {masked:?}?"))]
    HostBitsSet { value: String, masked: String },

    #[snafu(display(
        "{WILDCARD:?} trusts every peer, so it must be the only entry in the trusted proxy list; \
         remove the other entries or replace them all with {WILDCARD:?}"
    ))]
    WildcardMustBeSoleEntry,
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
            .with_context(|_| InvalidIpAddressSnafu {
                value: value.to_owned(),
            })?;

        if let Some(prefix_length) = prefix_length {
            let prefix_length =
                prefix_length
                    .parse::<u8>()
                    .with_context(|_| InvalidPrefixLengthSnafu {
                        value: value.to_owned(),
                    })?;

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

            // uvicorn parses this address with `ipaddress.ip_network(host, strict=True)`, which
            // rejects a network address with host bits set. When that happens uvicorn falls back
            // to treating the whole entry as an opaque string literal that matches no peer --
            // silently disabling proxy trust for exactly this entry. Reject it here instead, and
            // suggest the masked network the user probably meant.
            if let Some(masked) = masked_network(address, prefix_length) {
                ensure!(
                    masked == address,
                    HostBitsSetSnafu {
                        value,
                        masked: format!("{masked}/{prefix_length}"),
                    }
                );
            }
        }

        Ok(Self(value.to_owned()))
    }
}

/// The network address for `address/prefix_length`: `address` with every bit past
/// `prefix_length` cleared.
///
/// Returns `None` if `address` and `prefix_length` are not both IPv4 or both IPv6 -- which cannot
/// happen from `FromStr`, since `prefix_length`'s maximum is derived from `address`'s family, but
/// keeping the function total avoids relying on that invariant here.
fn masked_network(address: IpAddr, prefix_length: u8) -> Option<IpAddr> {
    match address {
        IpAddr::V4(addr) => {
            let bits = u32::from(addr);
            // A shift equal to the full width is undefined behaviour for `u32`/`u128`, so the
            // all-bits-masked-out case (prefix length 0) is handled separately.
            let mask = if prefix_length == 0 {
                0
            } else {
                u32::MAX << (32 - prefix_length)
            };
            Some(IpAddr::V4(Ipv4Addr::from(bits & mask)))
        }
        IpAddr::V6(addr) => {
            let bits = u128::from(addr);
            let mask = if prefix_length == 0 {
                0
            } else {
                u128::MAX << (128 - prefix_length)
            };
            Some(IpAddr::V6(Ipv6Addr::from(bits & mask)))
        }
    }
}

impl Display for TrustedProxy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Checks the list-level rule that a single `FromStr` call cannot see: uvicorn only ever trusts
/// every peer when the whole trusted-hosts value is exactly `*` (`trusted_hosts in ("*", ["*"])`
/// in uvicorn's own check), not when `*` merely appears alongside other entries. Combining `*`
/// with anything else is accepted by `FromStr` but silently degrades to trusting only the other
/// entries -- exactly the kind of silent surprise this feature exists to prevent.
pub fn ensure_wildcard_is_sole_entry(entries: &[TrustedProxy]) -> Result<(), Error> {
    let has_wildcard = entries.iter().any(|entry| entry.0 == WILDCARD);
    ensure!(
        !has_wildcard || entries.len() == 1,
        WildcardMustBeSoleEntrySnafu
    );
    Ok(())
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

    /// A too-large numeric prefix length must not be reported as "not a number" -- it is a
    /// number, just out of `u8` range, and the two failure modes need different messages.
    #[test]
    fn a_too_large_prefix_length_is_not_reported_as_non_numeric() {
        let error = TrustedProxy::from_str("10.0.0.0/300").expect_err("must be rejected");
        assert!(
            matches!(error, Error::InvalidPrefixLength { .. }),
            "error was: {error:?}"
        );
        assert!(
            !error.to_string().contains("is not a number"),
            "message was: {error}"
        );
    }

    #[rstest]
    #[case("10.244.0.1/16", "10.244.0.0/16")]
    #[case("fd00::1/8", "fd00::/8")]
    fn rejects_a_cidr_with_host_bits_set(#[case] value: &str, #[case] expected_masked: &str) {
        let error = TrustedProxy::from_str(value).expect_err("must be rejected");
        assert!(
            matches!(error, Error::HostBitsSet { .. }),
            "error was: {error:?}"
        );
        assert!(
            error.to_string().contains(expected_masked),
            "message was: {error}, expected it to suggest {expected_masked}"
        );
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

    #[test]
    fn wildcard_alone_is_accepted() {
        let entries = [TrustedProxy::from_str("*").expect("must be accepted")];
        ensure_wildcard_is_sole_entry(&entries).expect("a sole wildcard is fine");
    }

    #[test]
    fn wildcard_combined_with_another_entry_is_rejected() {
        let entries = [
            TrustedProxy::from_str("*").expect("must be accepted"),
            TrustedProxy::from_str("10.0.0.0/8").expect("must be accepted"),
        ];
        assert!(matches!(
            ensure_wildcard_is_sole_entry(&entries),
            Err(Error::WildcardMustBeSoleEntry)
        ));
    }

    #[test]
    fn a_list_without_a_wildcard_is_accepted() {
        let entries = [TrustedProxy::from_str("10.0.0.0/8").expect("must be accepted")];
        ensure_wildcard_is_sole_entry(&entries).expect("no wildcard involved");
    }

    /// The underlying parse error must survive as `source`, so it reaches logs and Kubernetes
    /// events rather than being discarded.
    #[test]
    fn invalid_ip_address_keeps_the_parse_error_as_source() {
        let error = TrustedProxy::from_str("not-an-ip").expect_err("must be rejected");
        match error {
            Error::InvalidIpAddress { source, .. } => {
                // Constructing this proves `source` is a real `AddrParseError`.
                let _: std::net::AddrParseError = source;
            }
            other => panic!("expected InvalidIpAddress, got {other:?}"),
        }
    }

    #[test]
    fn invalid_prefix_length_keeps_the_parse_error_as_source() {
        let error = TrustedProxy::from_str("10.244.0.0/sixteen").expect_err("must be rejected");
        match error {
            Error::InvalidPrefixLength { source, .. } => {
                let _: ParseIntError = source;
            }
            other => panic!("expected InvalidPrefixLength, got {other:?}"),
        }
    }
}
