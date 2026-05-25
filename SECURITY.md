# Security Policy

## Supported versions

Only the latest released version is supported with security fixes.

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security reports.

Use GitHub's private vulnerability reporting:
https://github.com/monedula-dev/monedula-metrics-reporter/security/advisories/new

We aim to acknowledge reports within 72 hours and provide a fix or mitigation for confirmed issues within 30 days, depending on severity.

## Scope

In scope:
- The published `monedula-metrics-reporter` artifact.
- Configuration parsing and validation in `OtlpMetricReporterConfig`.
- Resource and exporter lifecycle in `OtlpMetricReporter` and `MetricCollector`.
- Anything that could cause the reporter to interfere with Kafka broker / client availability.

Out of scope:
- Issues in third-party dependencies (OpenTelemetry SDK, gRPC, Kafka) — please report those upstream.
- The local `quickstart/` Docker Compose setup (not a production deployment target).
