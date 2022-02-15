# Changelog

## [Unreleased]

### Added

- BREAKING: Prometheus metrics enabled ([#51]); The `statsdExporterVersion` must
  be set in the cluster specification.

[#51]: https://github.com/stackabletech/airflow-operator/pull/51

## [0.2.0] - 2022-02-14

### Changed
- Fixed a bug in the namespace resolution for the Init job that resulted in it not being triggered in non-default
namespaces. ([#23]).

[#23]: https://github.com/stackabletech/airflow-operator/pull/23

- Added comments about the override of configuration properties and environment variables, and added code to pass the 
environment variables in the custom resource to the container, as this step was missing ([#42]).

[#42]: https://github.com/stackabletech/airflow-operator/pull/42

## [0.1.0] - 2022-02-03

### Added
- Added the initial implementation of the operator. The Init command - which takes the credentials from a secret - is 
required to set up the external database, and the webserver service will wait for this to be completed before declaring 
itself to be ready. ([#1]).

[#1]: https://github.com/stackabletech/airflow-operator/pull/1
