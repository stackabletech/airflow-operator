# Changelog

## [Unreleased]

## [0.1.0] - 2022-02-03


### Added
- Added the initial implementation of the operator. The Init command - which takes the credentials from a secret - is required to set up the external database, and the webserver service will wait for this to be completed before declaring itself to be ready. ([#1]).

[#1]: https://github.com/stackabletech/airflow-operator/pull/1
