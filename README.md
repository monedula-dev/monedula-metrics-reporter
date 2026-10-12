# monedula-metrics-reporter

[![CI](https://github.com/monedula-dev/monedula-metrics-reporter/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/monedula-dev/monedula-metrics-reporter/actions/workflows/ci.yml?query=branch%3Amain)
[![CodeQL](https://github.com/monedula-dev/monedula-metrics-reporter/actions/workflows/github-code-scanning/codeql/badge.svg?branch=main)](https://github.com/monedula-dev/monedula-metrics-reporter/security/code-scanning)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![Java 17+](https://img.shields.io/badge/Java-17%2B-blue.svg)](https://adoptium.net/)
[![Kafka 3.7+](https://img.shields.io/badge/Kafka-3.7%2B-blue.svg)](https://kafka.apache.org/)

A Kafka MetricsReporter plugin that ships broker, JVM and KIP-714 client metrics over OTLP; a collector outage never blocks Kafka.

**Documentation:** [monedula.dev/flock/docs/metrics-reporter](https://monedula.dev/flock/docs/metrics-reporter/), for the 0.10.0 release

- [Install the reporter on brokers](https://monedula.dev/flock/docs/metrics-reporter/how-to/install-on-brokers/), including how to verify the JAR's signature
- [Run the Grafana quickstart](https://monedula.dev/flock/docs/metrics-reporter/tutorials/grafana-quickstart/)
- [Collect Kafka client metrics with KIP-714](https://monedula.dev/flock/docs/metrics-reporter/how-to/collect-client-telemetry/)
- [Configuration keys](https://monedula.dev/flock/docs/metrics-reporter/reference/configuration/) and [self-monitoring metrics](https://monedula.dev/flock/docs/metrics-reporter/reference/self-monitoring-metrics/)
- [Compatibility](https://monedula.dev/flock/docs/metrics-reporter/concepts/compatibility/): the Java and Kafka versions it supports, and the version matrix CI tests
- [How it compares](https://monedula.dev/flock/docs/metrics-reporter/concepts/comparisons/) with jmx_exporter, the OpenTelemetry JMX Scraper, kafka_exporter, the Confluent Telemetry Reporter and Strimzi

## Install

Put the JAR on each broker's classpath:

```bash
VERSION=0.10.0
curl -L -o monedula-metrics-reporter-${VERSION}.jar \
  https://repo1.maven.org/maven2/dev/monedula/monedula-metrics-reporter/${VERSION}/monedula-metrics-reporter-${VERSION}.jar
cp monedula-metrics-reporter-${VERSION}.jar $KAFKA_HOME/libs/
```

Then point it at your OpenTelemetry Collector in `server.properties` and restart the broker:

```properties
metric.reporters=dev.monedula.metricsreporter.OtlpMetricReporter
otlp.metric.reporter.endpoint=http://otel-collector:4317
otlp.metric.reporter.transport=grpc
```

The same JAR also runs inside Java clients; see [Report metrics from Java clients](https://monedula.dev/flock/docs/metrics-reporter/how-to/report-client-metrics/).

## Try it

[`quickstart/`](quickstart/) runs three brokers with the reporter, an OpenTelemetry Collector, Prometheus and Grafana in Docker Compose. With Docker and JDK 17 or newer, from a clone of this repository:

```bash
./gradlew shadowJar
cd quickstart && docker compose up -d
# about 45 seconds later: Grafana on http://localhost:3000, dashboards in the Kafka folder
```

[Run the Grafana quickstart](https://monedula.dev/flock/docs/metrics-reporter/tutorials/grafana-quickstart/) walks through the dashboards and the demo clients.

## Changes on main since 0.10.0

The main branch has changes that are not released yet, listed under Unreleased in [CHANGELOG.md](CHANGELOG.md). The main one: the allow-list and the client-metric labels can change at runtime through dynamic broker configuration (KIP-226). The docs describe it in [Filter metrics with an allow-list](https://monedula.dev/flock/docs/metrics-reporter/how-to/filter-metrics/#in-builds-from-main-change-the-allow-list-at-runtime) and [Collect Kafka client metrics](https://monedula.dev/flock/docs/metrics-reporter/how-to/collect-client-telemetry/#in-builds-from-main-change-the-labels-at-runtime).

## Building from source

With JDK 17 or newer:

```bash
git clone https://github.com/monedula-dev/monedula-metrics-reporter.git
cd monedula-metrics-reporter
./gradlew shadowJar
```

The plugin JAR is `build/libs/monedula-metrics-reporter-<version>.jar`. The version is `0.1.0` unless you pass `-PreleaseVersion=<version>`, as the release workflow does. The build also copies the JAR to `build/quickstart-libs/monedula-metrics-reporter.jar` for the quickstart.

## Testing

```bash
./gradlew test        # unit and integration tests; the integration tests need Docker
./gradlew :e2e:test   # Kafka, an OpenTelemetry Collector and Prometheus, in Docker
```

## Contributing and security

Report bugs and request features in [GitHub issues](https://github.com/monedula-dev/monedula-metrics-reporter/issues). Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request, and [SECURITY.md](SECURITY.md) before reporting a vulnerability. [CHANGELOG.md](CHANGELOG.md) lists the changes in each release.

## License

monedula-metrics-reporter is licensed under the AGPL-3.0; see [LICENSE](LICENSE) and [NOTICE](NOTICE). Running it in your own brokers and clients does not oblige you to share your code or configuration: AGPL obligations apply when you distribute it, modified or not, or let others use a modified version over a network.

Apache Kafka, Kafka and the Kafka logo are trademarks of The Apache Software Foundation. monedula-metrics-reporter is an independent project that works with Kafka; it is not affiliated with, endorsed by or sponsored by the ASF.
