# Architectural Assumptions

This project is built around a small set of explicit assumptions. They are
intentional product and engineering trade-offs, not incidental implementation
details.

## Runtime Assumptions

- The reporter can be installed on the Kafka broker or client JVM classpath.
- The host process runs Java 17 or newer.
- The supported Kafka line is Apache Kafka 3.x and 4.x.
- Broker-side deployment has access to Kafka broker classes, including the
  Yammer/Coda Hale metrics registry bridge used by Kafka internals.
- Client-side deployment may only have `kafka-clients` available; broker-only
  integrations must degrade cleanly when absent.
- The configured OTLP collector endpoint may be temporarily unavailable,
  overloaded, slow, or misconfigured.

## Failure Assumptions

- Kafka availability is more important than metrics completeness.
- Kafka metric callbacks must never perform network I/O.
- Export failure should drop the current batch rather than block Kafka work.
- The reporter should keep bounded memory usage; it stores latest observed
  metric objects, not a retry queue.
- Invalid static configuration must not crash Kafka. Kafka rethrows
  `MetricsReporter#configure` exceptions from `AbstractConfig.getConfiguredInstances`,
  so the reporter logs validation failures at ERROR, degrades to no-op for the
  rest of the JVM lifecycle, and lets the broker keep serving traffic.
- Runtime exporter failures after successful startup should be fail-open:
  log, drop the batch, and try again on the next interval.
- Collector authentication and TLS settings are static startup configuration.
  Missing header values or unreadable certificate/key files are operator
  mistakes, so they should be logged clearly while still preserving Kafka
  availability through no-op degradation.

## Metric Model Assumptions

- Kafka observability requires both metric systems Kafka exposes:
  Kafka's `MetricsReporter` SPI and the legacy Yammer/Coda Hale registry.
- Kafka SPI metrics are the best source for client metrics and many broker
  request/network metrics.
- Yammer metrics are still required for important broker internals such as
  active controller count, offline partitions, under-replicated partitions,
  and broker topic metrics.
- Kafka `MetricsContext` is the source of broker identity labels such as
  `kafka.cluster.id`, `kafka.node.id`, `kafka.broker.id`, and Kafka version.
- The `_namespace` context label belongs in the metric name prefix, not as a
  resource attribute.
- User-supplied resource attributes override Kafka context attributes when the
  same key appears in both places.
- Prometheus label names are expected to be sanitized by the collector/exporter
  path, so dotted resource keys such as `kafka.node.id` become labels such as
  `kafka_node_id`.

## Operational Assumptions

- OTLP is the integration boundary. Prometheus, Grafana, and vendor backends
  should be reached through the OpenTelemetry collector.
- Collector-facing auth headers, gzip compression, custom CA trust, and mTLS
  are reporter concerns because they affect the immediate OTLP connection.
  Credential rotation, retries, fan-out, and backend routing remain collector
  concerns.
- The quickstart uses the collector's Prometheus exporter and Prometheus pull
  scraping so metric type metadata is preserved.
- In environments where Prometheus cannot scrape the collector, remote write
  or direct vendor OTLP export is a deployment choice outside the reporter.
- Users may already scrape JVM metrics elsewhere, so JVM runtime metrics must
  remain configurable.
- Java Flight Recorder may be unavailable or unable to create its local
  repository; in that case JMX-backed runtime metrics should continue when
  possible.
- The reporter should not require JMX remote access, JMX rule YAML, or a broker
  HTTP scrape endpoint.
