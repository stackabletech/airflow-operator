//! Additions to stackable-operator
//!
//! Functions in stackable-operator usually accept generic types like strings and validate the
//! parameters as late as possible. Therefore, nearly all functions have to return a [`Result`] and
//! errors are returned along the call chain. That makes error handling complex because every
//! module re-packages the received error. Also, the validation is repeated if the value is used in
//! different function calls. Sometimes, validation is not necessary if constant values are used,
//! e.g. the name of the operator.
//!
//! This operator uses a different approach. The incoming values are validated as early as possible
//! and wrapped in a fail-safe type. This type is then used along the call chain, validation is not
//! necessary anymore and functions without side effects do not need to return a [`Result`].
//!
//! However, this operator uses stackable-operator and at the interface, the fail-safe types must
//! be unwrapped and the [`Result`] returned by the stackable-operator function must be handled.
//! This is done by calling [`Result::expect`] which requires thorough testing.
//!
//! When the development of this module has progressed and changes become less frequent, then this
//! module can be incorporated into stackable-operator. The module structure should already
//! resemble the one of stackable-operator.

use types::kubernetes::Uid;

#[allow(dead_code)]
pub mod builder;
#[allow(dead_code)]
pub mod cluster_resources;
#[allow(dead_code)]
pub mod controller_utils;
#[allow(dead_code)]
pub mod kvp;
pub mod macros;
#[allow(dead_code)]
pub mod product_logging;
#[allow(dead_code)]
pub mod role_group_utils;
#[allow(dead_code)]
pub mod role_utils;
pub mod types;

/// Has a non-empty name
///
/// Useful as an object reference; Should not be used to create an object because the name could
/// violate the naming constraints (e.g. maximum length) of the object.
pub trait HasName {
    #[allow(dead_code)]
    fn to_name(&self) -> String;
}

/// Has a Kubernetes UID
#[allow(dead_code)]
pub trait HasUid {
    fn to_uid(&self) -> Uid;
}

/// The name is a valid label value
#[allow(dead_code)]
pub trait NameIsValidLabelValue {
    fn to_label_value(&self) -> String;
}
