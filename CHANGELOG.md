# Changelog

## [Unreleased]

### Added

- Cpu and memory limits are now configurable ([#167]).

### Changed

- Toggle podSecurityContext for Openshift/Non-Openshift test scenarios ([#171])

[#167]: https://github.com/stackabletech/airflow-operator/pull/167
[#171]: https://github.com/stackabletech/airflow-operator/pull/171

## [0.5.0] - 2022-09-06

### Added

- LDAP user authentication ([#133]).
- Documentation for calling custom resources ([#155]).

### Changed

- OpenShift compatibility ([#127])
- Include chart name when installing with a custom release name ([#131], [#132]).
- Use correct webserver key to allow access to DAG logs from the Webserver UI ([#155]).
- Add LDAP readiness probe for tests ([#155]).

[#127]: https://github.com/stackabletech/airflow-operator/pull/127
[#131]: https://github.com/stackabletech/airflow-operator/pull/131
[#132]: https://github.com/stackabletech/airflow-operator/pull/132
[#133]: https://github.com/stackabletech/airflow-operator/pull/133
[#155]: https://github.com/stackabletech/airflow-operator/pull/155

## [0.4.0] - 2022-06-30

## [0.3.0] - 2022-05-31

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
- Stabilize integration tests ([#109]).

### Changed

- `operator-rs` `0.10.0` -> `0.18.0` ([#53],[#54],[#89]).
- [BREAKING] Specifying the product version has been changed to adhere to [ADR018](https://docs.stackable.tech/home/contributor/adr/ADR018-product_image_versioning.html) instead of just specifying the product version you will now have to add the Stackable image version as well, so `version: 3.5.8` becomes (for example) `version: 3.5.8-stackable0.1.0` ([#106])

[#51]: https://github.com/stackabletech/airflow-operator/pull/51
[#53]: https://github.com/stackabletech/airflow-operator/pull/53
[#54]: https://github.com/stackabletech/airflow-operator/pull/54
[#68]: https://github.com/stackabletech/airflow-operator/pull/68
[#84]: https://github.com/stackabletech/airflow-operator/pull/84
[#89]: https://github.com/stackabletech/airflow-operator/pull/89
[#104]: https://github.com/stackabletech/airflow-operator/pull/104
[#106]: https://github.com/stackabletech/airflow-operator/pull/106
[#109]: https://github.com/stackabletech/airflow-operator/pull/109

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
