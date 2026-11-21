# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- The OpenTelemetry stack moved to SDK 1.68.0 and instrumentation 2.34.0-alpha.
  What had pinned `otelVersion` to 1.61.0 was the artifact, not the SDK:
  `opentelemetry-runtime-telemetry-java17` was superseded by the unified
  `opentelemetry-runtime-telemetry` module and its last release is 2.27.0-alpha,
  whose POM targets core 1.61.0. The per-Java-version aliases were removed
  upstream in instrumentation 2.28.0. This release switches to the unified module,
  which puts `otelVersion` back in lockstep with the core release
  `otelInstrumentationVersion` was built against - 2.34.0-alpha targets 1.68.0 -
  so the BOM-alignment rule documented in 0.20.0 still holds.
- `JvmMetricsLifecycle` now drives `RuntimeTelemetry` in place of
  `RuntimeMetrics`, and no longer attempts the JFR-enabled start twice. The
  unified module builds its JMX observables independently of its JFR stream and
  returns a JMX-only handle when JFR cannot start - a read-only `/tmp` in a slim
  container, say - so one `RuntimeTelemetry.create()` covers what the
  `disableAllFeatures()` retry used to. The reporter's fail-open behaviour is
  unchanged: a start failure is logged and the broker keeps running without JVM
  metrics.
- **The instrumentation scope on JVM runtime metrics changed.** Upstream renamed
  it from `io.opentelemetry.runtime-telemetry-java17` to
  `io.opentelemetry.runtime-telemetry` when it unified the module. Note what that
  means for this plugin specifically: `shadowJar` rewrites the scope-name string
  constant along with the package it relocates, so what the published jar emits is
  the relocated form -
  `dev.monedula.metricsreporter.shaded.opentelemetry.runtime-telemetry`, where it
  used to be `...shaded.opentelemetry.runtime-telemetry-java17`. That relocated
  name, not the upstream one, is what collector processors, views, dashboards and
  alerts have to match. The scope now also carries the schema URL
  `https://opentelemetry.io/schemas/1.44.0`.
- The JVM metric set itself is unchanged. An e2e run against a real broker,
  collector and Prometheus shows `jvm.class.count`, `jvm.class.loaded`,
  `jvm.class.unloaded`, `jvm.cpu.count`, `jvm.cpu.recent_utilization`,
  `jvm.cpu.time`, `jvm.gc.duration`, `jvm.memory.committed`, `jvm.memory.limit`,
  `jvm.memory.used`, `jvm.memory.used_after_last_gc` and `jvm.thread.count`
  arriving as before.
- The unified module is a multi-release jar, so the shadow jar now declares
  `Multi-Release: true` and carries its `META-INF/versions/17/` classes relocated
  under `dev.monedula.metricsreporter.shaded.*` alongside the base ones.
- Two of the transitive libraries that *do* ship inside the shadow jar moved with
  the SDK bump: `okhttp` 5.3.2 -> 5.5.1 and `okio` 3.16.4 -> 3.18.1.
  `kotlin-stdlib` stays at 2.2.21. `opentelemetry-semconv` moved 1.40.0 -> 1.45.0.
- ASM in the `:benchmarks` jmh configuration moved 9.0 -> 9.11.1. JMH 1.37 still
  resolves `jmh-generator-asm` against ASM 9.0 (2020), which cannot read class
  files newer than Java 15 and reads as a stale dependency in scans. A constraint
  in `benchmarks/build.gradle` (`asmVersion` in `gradle.properties`) pulls it up
  to the release JaCoCo's tooling already resolves. The generator only reads the
  benchmark classes this project compiles at release 17; `jmhRunBytecodeGenerator`
  and `jmhJar` were verified against the new version. Nothing here reaches the
  published jar - the jmh configuration is build-time only.
- Remaining dependency refresh: JUnit 5.14.4 -> 6.1.3 (JUnit 6 requires Java 17,
  which the project already floors at), the `zstd-jni` floor 1.5.7-26 -> 1.5.7-28,
  and the CycloneDX Gradle plugin 3.6.0 -> 3.6.1 (the 3.5.1 -> 3.6.0 step and the
  Gradle wrapper 9.8.2 -> 9.9.1 arrived on main separately). OpenTelemetry proto,
  Mockito, Testcontainers, SLF4J, JaCoCo, the `snappy-java` floor and the other
  Gradle plugins were already current. The default `kafkaVersion` deliberately
  stays at 4.2.2 and the `lz4-java` floor at 1.11.2, the version Kafka 4.4.0
  ships.
- The `gradle.lockfile`s in the root, `:e2e` and `:benchmarks` projects were
  regenerated for all of the above.

### Security

- Every `actions/checkout` step in `ci.yml`, `benchmarks.yml` and `release.yml`
  now sets `persist-credentials: false`. The action's default leaves the job's
  `GITHUB_TOKEN` in the runner's git config for the rest of the run, where any
  later step or third-party action can read it. No workflow here pushes with that
  token - the release job passes `GITHUB_TOKEN` explicitly to the steps that need
  it - so nothing depends on the persisted credential.

## [0.20.0] - 2026-11-13

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
- The CI matrix now also covers Apache Kafka 4.4.0, the current latest minor,
  alongside 3.7.2, 3.9.2, 4.2.2 and 4.3.2 (15 cells across JDK 17, 21 and 25).
  The compiled-against default stays at 4.2.2 and the supported range is
  unchanged: Java 17 or newer, Apache Kafka 3.7 or newer.
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
