# Contributing

Thanks for considering a contribution to `monedula-metrics-reporter`.

## Building locally

Requirements:
- JDK 17+ (the project compiles to bytecode targeting 17 and is tested on 17 and 21).
- Docker — required only for the `:e2e:test` end-to-end suite and the Testcontainers-based `OtlpExportIntegrationTest`. Unit tests run fine without it.

```
./gradlew shadowJar     # builds the relocated, classpath-safe plugin jar
./gradlew test          # unit + integration tests
./gradlew :e2e:test     # full Kafka + OTel collector + Prometheus end-to-end
./gradlew :benchmarks:jmh :benchmarks:test  # JMH + soak + timeout benchmarks (~10 min)
```

## Writing tests

Most existing tests construct `KafkaMetric` via `Mockito.mock(KafkaMetric.class)`. That's fine for unit tests with bounded invocation counts. **Do not use Mockito mocks for hot-path benchmarks or long-running soak tests** — Mockito's ByteBuddy proxy adds ~15 µs per `metricName()` call (dominates nanosecond-scale measurements) and records every invocation on every mock for later verification (accumulates hundreds of MB during multi-minute runs and OOMs the test JVM). Construct real `KafkaMetric` instances via the public 5-arg constructor — see [`MetricChangeBenchmark`](benchmarks/src/jmh/java/dev/monedula/metricsreporter/bench/MetricChangeBenchmark.java) and [`BoundedMemorySoakTest`](benchmarks/src/test/java/dev/monedula/metricsreporter/bench/BoundedMemorySoakTest.java) for the pattern.

## Picking up an issue

- Comment on the issue first so we can avoid duplicate work.
- For non-trivial changes, sketch the approach in the issue before writing code.

## Pull request expectations

- One logical change per PR. Refactors stay separate from behavior changes.
- All CI jobs must pass: unit tests, integration tests, e2e, the JDK/Kafka version matrix, the Gradle wrapper validation, `spotlessCheck`, and the dependency-review check.
- Run `./gradlew spotlessApply` before opening the PR. CI's `spotlessCheck` will reject any unformatted code; running locally first avoids a round-trip.
- New behavior comes with a test that fails without the change.
- Keep public-API changes (anything under `dev.monedula.metricsreporter` that an integrator could see) backwards-compatible when feasible. If you must break, call it out explicitly in the PR description.
- Don't reformat untouched code beyond what `spotlessApply` does. Style follows palantir-java-format.

## Commit and PR style

- Commits in the imperative mood ("add foo", not "added foo" or "adds foo").
- PR title under 70 characters. Use the body for detail.
- Link the issue: `Resolves #123`.

## What goes in this project

In scope:
- Bridging Kafka SPI and Yammer registries to OTLP.
- Broker / client lifecycle that respects the "Kafka availability > metrics completeness" assumption — see [docs/assumptions.md](docs/assumptions.md).

Out of scope:
- Consumer lag, offset age, topic inventory (use `kafka_exporter`).
- Auth, TLS, compression, fan-out, vendor export (belongs in the OpenTelemetry collector).
- Replacing the collector.

If you're unsure whether something fits, open a discussion before writing code.

## Reporting security issues

Don't open a public issue. See [SECURITY.md](SECURITY.md).
