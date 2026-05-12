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

#[macro_use]
pub mod macros;
pub mod types;

/// The name is a valid label value
pub trait NameIsValidLabelValue {
    fn to_label_value(&self) -> String;
}
