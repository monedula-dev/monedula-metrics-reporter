# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Allow-list patterns are evaluated independently rather than folded into one
  regex alternation, so a backreference in one pattern can no longer be corrupted
  by capture-group renumbering across patterns.

### Added

- Initial release of the OTLP-native Kafka `MetricsReporter` plugin
  (`dev.monedula.metricsreporter.OtlpMetricReporter`).
- Bridges both Kafka metric streams with a single configuration: the Kafka
  `MetricsReporter` SPI (clients + broker SPI metrics) and the legacy
  Yammer/Coda Hale registry (broker-internal metrics such as
  `UnderReplicatedPartitions`, `OfflinePartitionsCount`,
  `ActiveControllerCount`, `BrokerTopicMetrics`). The same JAR runs unchanged
  on clients — the Yammer attach silently no-ops when broker classes are
  absent.
- JVM runtime metrics via `opentelemetry-runtime-telemetry-java17` (memory,
  GC, threads, classes, CPU). Disable via
  `otlp.metric.reporter.jvm.metrics.enabled=false`. Falls back to JMX-only
  metrics if JFR cannot start in the host JVM.
- Broker context (cluster id, broker/node id, Kafka version) attached as
  OTLP resource attributes (rendered as Prometheus labels on every series).
- Fail-safe export path: metric callbacks only touch an in-memory
  `ConcurrentHashMap`; export runs on a daemon scheduler thread with a
  per-call timeout. Unreachable or slow collectors cannot stall Kafka.
- Collector authentication and TLS options: static request headers
  (`otlp.metric.reporter.headers`), gzip compression
  (`otlp.metric.reporter.compression`), custom CA bundle
  (`otlp.metric.reporter.trusted.certificates.path`), and mTLS
  (`otlp.metric.reporter.client.certificate.path` +
  `otlp.metric.reporter.client.key.path`).
- Allow-listing via comma-separated regex patterns
  (`otlp.metric.reporter.allowed.metrics`).
- `:benchmarks` Gradle subproject with three suites backing load-bearing
  doc claims: JMH callback hot-path microbenchmark, 5-minute heap-soak
  test against a dead collector, slow-collector timeout test via
  Testcontainers + Toxiproxy. Nightly CI workflow with on-demand dispatch.
- `:e2e` Gradle subproject: end-to-end test against Kafka 4.2.0 + OTel
  collector + Prometheus via Testcontainers, asserting producer/broker/JVM
  metrics land in Prometheus with proper type metadata and broker-context
  labels.
- Quickstart Docker Compose stack with 3-broker KRaft Kafka, OTel
  collector, Prometheus, and Grafana pre-provisioned with five dashboards
  (overview, production, consumption, topic details, JVM health).
- CI matrix: JDK 17 + 21 × Kafka 3.7.0 / 3.9.0 / 4.2.0 / 4.3.0.

### Security

- AGPL-3.0 licensed.
- Private vulnerability reporting via GitHub Security Advisories
  ([SECURITY.md](SECURITY.md)).
- Third-party deps shaded inside the published JAR with full attribution
  ([NOTICE](NOTICE)).
- Gradle wrapper jar validated in CI on every PR.
- Dependency review on every PR; fails the check on high-severity
  vulnerabilities.

### Notes

This is pre-1.0 software. Public API (anything under
`dev.monedula.metricsreporter` an integrator could see) is considered
stable, but breaking changes are still possible before the 1.0 cut. See
[CONTRIBUTING.md](CONTRIBUTING.md) for the project's compatibility stance.
