# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Security

- Build dependencies are now locked. Every resolvable configuration in the root,
  `:e2e` and `:benchmarks` projects is pinned by a checked-in `gradle.lockfile`,
  so a republished or hijacked upstream version cannot silently enter a build.
  Regenerate with `./gradlew resolveAndLockAll --write-locks` after changing a
  dependency. The `org.apache.kafka:*` and lz4 coordinates are excluded from the
  lock state because CI resolves them at four different Kafka versions.
- Every GitHub Actions step is pinned to a commit SHA rather than a mutable tag,
  so a retagged third-party action cannot alter a CI or release run. Dependabot
  keeps the pins and their version comments current.
- `release.yml` defaults to `contents: read` and re-grants `contents: write`,
  `packages: write` and `id-token: write` on the `publish` job alone, so any job
  added later starts least-privileged.
- The OpenTelemetry stack moved from 1.40.0 (June 2024) to 1.61.0, which carries
  `okhttp` 4.12.0 -> 5.3.2 and with it `kotlin-stdlib` 1.9.10 -> 2.2.21. Unlike the
  Kafka-side findings below, these two *do* ship inside the shadow jar (relocated
  under `dev.monedula.metricsreporter.shaded.*`), so this is the one upgrade here
  that changes what consumers actually receive.
- Version floors for the native compression codecs `kafka-clients` pulls in:
  `snappy-java` >= 1.1.10.12, `zstd-jni` >= 1.5.7-26, `lz4-java` >= 1.11.2. These
  clear the 2026 double-free and out-of-bounds-read advisories in those
  libraries. Note that `kafka-clients` is `compileOnly`/test-only here — the
  broker supplies it and these natives at runtime, so none of it ever reached the
  published jar; the floors protect the CI test JVM and keep dependency scans
  clean.

### Changed

- The default `kafkaVersion` moved from 4.2.1 to 4.2.2, and the CI matrix from
  3.7.2, 3.9.2, 4.2.1, 4.3.1 to 3.7.2, 3.9.2, 4.2.2, 4.3.2 — the patch releases
  that carry Apache Kafka's OAUTHBEARER JWT signature-validation and
  `FetchSnapshot` DoS fixes. The quickstart `docker-compose.yml` and the e2e
  harness moved to the `apache/kafka:4.2.2` broker image for the same reason. The
  supported range is unchanged: Java 17 or newer, Apache Kafka 3.7 or newer.
- Dependency refresh. Several versions had drifted badly because Dependabot does
  not resolve the `${name}Version` property references in `gradle.properties`:
  OpenTelemetry SDK 1.40.0 -> 1.61.0, OpenTelemetry instrumentation 2.7.0-alpha ->
  2.27.0-alpha, OpenTelemetry proto 1.7.0-alpha -> 1.11.1-alpha, JUnit 5.10.2 ->
  5.14.4, Mockito 5.23.0 -> 5.24.0, SLF4J 2.0.18 -> 2.0.20, JaCoCo 0.8.13 ->
  0.8.16. The Gradle plugins were already current.
- The root build now imports `opentelemetry-bom` and `opentelemetry-bom-alpha`.
  `otelVersion` must track the core release that `otelInstrumentationVersion` was
  built against: the instrumentation artifacts pull in
  `opentelemetry-api-incubator` at their own core version, and a skew between that
  and `opentelemetry-sdk-metrics` is not a compile error - it surfaces at runtime
  as a `NoClassDefFoundError` inside `PeriodicMetricReader`, which the reporter's
  fail-open handling turns into silently missing JVM metrics.

## [0.11.0] - 2026-10-20

### Added

- Live reconfiguration of metric filtering and client-telemetry enrichment via
  Kafka dynamic broker configuration (KIP-226): `OtlpMetricReporter` now
  implements `Reconfigurable`, so `otlp.metric.reporter.allowed.metrics` and the
  four `otlp.metric.reporter.client.telemetry.enrich.*` toggles (`.broker`,
  `.client.identity`, `.client.id`, `.client.instance.id`) can be changed at
  runtime with `kafka-configs.sh --entity-type brokers --alter` (or an
  AdminClient `incrementalAlterConfigs`) — no broker restart. Changes apply
  cluster-wide (`--entity-default`) or per broker (`--entity-name`), persist in
  cluster metadata, override `server.properties`, and are removed with
  `--delete-config`. Invalid values (a bad regex or non-boolean toggle) are
  rejected by the `--alter` command itself: the broker validates before applying,
  so a bad change never touches the running export pipeline. All other
  `otlp.metric.reporter.*` keys remain static and require a restart.

### Changed

- Allow-list filtering now happens at export time on the collector's daemon
  thread instead of at ingest in the registries. The registries retain
  references to every Kafka metric the broker announces (memory stays bounded by
  the broker's own metric count), which lets a dynamic widening of
  `allowed.metrics` resurrect metrics Kafka announced only once at startup. As a
  side effect, regex matching no longer runs on Kafka's hot metric-callback path.
- CI runs the unit and integration tests on JDK 25 as well as 17 and 21, against
  the latest Kafka patch releases: 3.7.2, 3.9.2, 4.2.1 and 4.3.1. The end-to-end
  test runs against Kafka 4.2.1. The supported range is unchanged: Java 17 or
  newer, Apache Kafka 3.7 or newer.
- The documentation moved to https://monedula.dev/flock/docs/metrics-reporter/,
  and the README is now a short overview that links to it.

## [0.10.0] - 2026-07-10

### Added

- KIP-714 client telemetry forwarding: the reporter now implements Kafka's
  `ClientTelemetry` interface, receives the OTLP metrics that KIP-714 clients
  push to the broker, and forwards them to the collector as a third metric
  stream alongside the SPI and Yammer registries. Forwarded client metrics are
  enriched with broker identity (`kafka_cluster_id`/`kafka_node_id`) and
  `client_id` by default; `client_instance_id`, principal, and client address
  are opt-in. New config keys under `otlp.metric.reporter.client.telemetry.*`,
  five `monedula_reporter_clienttelemetry_*` self-monitoring metrics, two
  Grafana client dashboards (producer/consumer, split into Standard vs
  Extended panels by client-library portability), and quickstart demo clients
  (Java by default, librdkafka/confluent-kafka-python behind a compose
  profile).

### Changed

- The default OTLP endpoint now follows the configured transport: gRPC defaults
  to `http://localhost:4317`, HTTP to `http://localhost:4318` (previously HTTP
  also defaulted to the gRPC :4317 port, producing an unreachable endpoint).
- Kafka SPI cumulative counters (metrics named `*-total`) are now exported as
  monotonic OTLP Sums instead of gauges, so PromQL `rate()` over them is
  semantically correct. All other SPI metrics remain gauges.

### Fixed

- **Metric rename:** the export-duration self-metric now reaches Prometheus as
  `monedula_reporter_export_duration_ms` — the name the README and the
  quickstart dashboards/alerts always documented. In 0.9.0 the metric carried a
  redundant OTLP `ms` unit that made the collector's Prometheus exporter append
  a unit suffix, so it was actually scraped as
  `monedula_reporter_export_duration_ms_milliseconds`. Any dashboard, recording
  rule, or alert built against that suffixed name must be updated to the
  documented name after upgrading.
- The HTTP endpoint metrics-path normalization now preserves any query string or
  fragment instead of clobbering it via string concatenation.
- Export no longer stops permanently if a tick throws an `Error` (e.g. a
  first-tick `NoClassDefFoundError`): the scheduled task now catches `Throwable`,
  logs, and retries on the next interval instead of being silently cancelled by
  `scheduleAtFixedRate`.
- Allow-list patterns are evaluated independently rather than folded into one
  regex alternation, so a backreference in one pattern can no longer be corrupted
  by capture-group renumbering across patterns.
- A metric value that flips to non-finite/non-numeric between snapshot and
  mapping no longer drops the entire export batch. Values are captured once at
  snapshot time, which also halves the `synchronized` `metricValue()` reads per
  tick.
- Per-metric rendering/name/scope caches are pruned to the live metric set on the
  export thread, fixing two unbounded leaks under metric/topic churn (the Yammer
  scope-attribute cache and a removal-vs-export race on the SPI rendering cache)
  and keeping all cache maintenance off Kafka's metric-callback path.
- Cumulative-counter `startEpochNanos` stays stable across a `contextChange`
  mapper rebuild, so a label change no longer looks like a counter reset to
  downstream tools.


## [0.9.0] - 2026-05-27

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
