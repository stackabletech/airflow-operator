# Changelog

## [Unreleased]

### Added

- BREAKING: Prometheus metrics enabled ([#51]); The `statsdExporterVersion` must
  be set in the cluster specification.
- Reconciliation errors are now reported as Kubernetes events ([#53]).
- Use cli argument `watch-namespace` / env var `WATCH_NAMESPACE` to specify
  a single namespace to watch ([#54]).
- Support for Airflow 2.2.4, 2.2.5 documented ([#68],[#84]).
- Support for mounting DAGs via `ConfigMap` or `PersistentVolumeClaim` ([#84]).
- Init job replaced with an AirflowDB resource created by the operator  ([#89]).
- Stabilize start-up by watching AirflowDB job ([#104]).

### Changed

- `operator-rs` `0.10.0` -> `0.19.0` ([#53],[#54],[#89]).

[#51]: https://github.com/stackabletech/airflow-operator/pull/51
[#53]: https://github.com/stackabletech/airflow-operator/pull/53
[#54]: https://github.com/stackabletech/airflow-operator/pull/54
[#68]: https://github.com/stackabletech/airflow-operator/pull/68
[#84]: https://github.com/stackabletech/airflow-operator/pull/84
[#89]: https://github.com/stackabletech/airflow-operator/pull/89
[#104]: https://github.com/stackabletech/airflow-operator/pull/104

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
