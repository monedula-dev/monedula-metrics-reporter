// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter;

import static java.util.Map.entry;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

class OtlpMetricReporterConfigTest {

    @Test
    void defaults_applied_when_config_is_empty() {
        var cfg = new OtlpMetricReporterConfig(Map.of());
        assertEquals("http://localhost:4317", cfg.endpoint());
        assertEquals("grpc", cfg.transport());
        assertEquals(30_000L, cfg.intervalMs());
        assertEquals(5_000L, cfg.timeoutMs());
        assertTrue(cfg.allowedMetrics().isEmpty());
        assertTrue(cfg.resourceAttributes().isEmpty());
        assertTrue(cfg.headers().isEmpty());
        assertEquals("none", cfg.compression());
        assertTrue(cfg.trustedCertificatesPath().isEmpty());
        assertTrue(cfg.clientCertificatePath().isEmpty());
        assertTrue(cfg.clientKeyPath().isEmpty());
        assertTrue(cfg.jvmMetricsEnabled());
    }

    @Test
    void defaults_map_round_trips_through_constructor() {
        // defaults() must be internally consistent: building a config from those
        // values must yield the same accessor results as building from an empty
        // map (the "no overrides" baseline). This protects against a key/value
        // drifting from its constructor-side default.
        var fromEmpty = new OtlpMetricReporterConfig(Map.of());
        var fromDefaults = new OtlpMetricReporterConfig(OtlpMetricReporterConfig.defaults());
        assertEquals(fromEmpty.endpoint(), fromDefaults.endpoint());
        assertEquals(fromEmpty.transport(), fromDefaults.transport());
        assertEquals(fromEmpty.intervalMs(), fromDefaults.intervalMs());
        assertEquals(fromEmpty.timeoutMs(), fromDefaults.timeoutMs());
        assertEquals(fromEmpty.compression(), fromDefaults.compression());
        assertEquals(fromEmpty.jvmMetricsEnabled(), fromDefaults.jvmMetricsEnabled());
    }

    @Test
    void defaults_map_contains_every_known_key() {
        // If a new public constant gets added to OtlpMetricReporterConfig without a
        // corresponding entry in defaults(), this test catches the omission.
        assertTrue(OtlpMetricReporterConfig.defaults().containsKey(OtlpMetricReporterConfig.ENDPOINT));
        assertTrue(OtlpMetricReporterConfig.defaults().containsKey(OtlpMetricReporterConfig.TRANSPORT));
        assertTrue(OtlpMetricReporterConfig.defaults().containsKey(OtlpMetricReporterConfig.INTERVAL_MS));
        assertTrue(OtlpMetricReporterConfig.defaults().containsKey(OtlpMetricReporterConfig.TIMEOUT_MS));
        assertTrue(OtlpMetricReporterConfig.defaults().containsKey(OtlpMetricReporterConfig.ALLOWED_METRICS));
        assertTrue(OtlpMetricReporterConfig.defaults().containsKey(OtlpMetricReporterConfig.RESOURCE_ATTRS));
        assertTrue(OtlpMetricReporterConfig.defaults().containsKey(OtlpMetricReporterConfig.HEADERS));
        assertTrue(OtlpMetricReporterConfig.defaults().containsKey(OtlpMetricReporterConfig.COMPRESSION));
        assertTrue(OtlpMetricReporterConfig.defaults().containsKey(OtlpMetricReporterConfig.TRUSTED_CERTIFICATES_PATH));
        assertTrue(OtlpMetricReporterConfig.defaults().containsKey(OtlpMetricReporterConfig.CLIENT_CERTIFICATE_PATH));
        assertTrue(OtlpMetricReporterConfig.defaults().containsKey(OtlpMetricReporterConfig.CLIENT_KEY_PATH));
        assertTrue(OtlpMetricReporterConfig.defaults().containsKey(OtlpMetricReporterConfig.JVM_METRICS_ENABLED));
    }

    @Test
    void defaults_map_is_immutable() {
        assertThrows(UnsupportedOperationException.class, () -> OtlpMetricReporterConfig.defaults()
                .put("any", "value"));
    }

    @Test
    void jvm_metrics_enabled_defaults_to_true() {
        assertTrue(new OtlpMetricReporterConfig(Map.of()).jvmMetricsEnabled());
    }

    @Test
    void jvm_metrics_can_be_disabled() {
        assertFalse(new OtlpMetricReporterConfig(Map.of("otlp.metric.reporter.jvm.metrics.enabled", "false"))
                .jvmMetricsEnabled());
    }

    @Test
    void custom_values_parsed_correctly() {
        var cfg = new OtlpMetricReporterConfig(Map.ofEntries(
                entry("otlp.metric.reporter.endpoint", "http://collector:4318"),
                entry("otlp.metric.reporter.transport", "http"),
                entry("otlp.metric.reporter.interval.ms", "10000"),
                entry("otlp.metric.reporter.timeout.ms", "2000"),
                entry("otlp.metric.reporter.allowed.metrics", "producer-metrics\\..*,consumer.*"),
                entry("otlp.metric.reporter.resource.attributes", "env=prod,team=platform"),
                entry("otlp.metric.reporter.headers", "Authorization=Bearer token,x-tenant=monedula"),
                entry("otlp.metric.reporter.compression", "gzip"),
                entry("otlp.metric.reporter.trusted.certificates.path", "/etc/ssl/collector-ca.pem"),
                entry("otlp.metric.reporter.client.certificate.path", "/etc/ssl/client-cert.pem"),
                entry("otlp.metric.reporter.client.key.path", "/etc/ssl/client-key.pem")));
        assertEquals("http://collector:4318/v1/metrics", cfg.endpoint());
        assertEquals("http", cfg.transport());
        assertEquals(10_000L, cfg.intervalMs());
        assertEquals(2_000L, cfg.timeoutMs());
        assertEquals(2, cfg.allowedMetrics().size()); // two compiled Pattern objects
        assertEquals(Map.of("env", "prod", "team", "platform"), cfg.resourceAttributes());
        assertEquals(Map.of("Authorization", "Bearer token", "x-tenant", "monedula"), cfg.headers());
        assertEquals("gzip", cfg.compression());
        assertEquals("/etc/ssl/collector-ca.pem", cfg.trustedCertificatesPath().orElseThrow());
        assertEquals("/etc/ssl/client-cert.pem", cfg.clientCertificatePath().orElseThrow());
        assertEquals("/etc/ssl/client-key.pem", cfg.clientKeyPath().orElseThrow());
    }

    @Test
    void header_values_may_contain_equals_signs() {
        var cfg = new OtlpMetricReporterConfig(Map.of("otlp.metric.reporter.headers", "Authorization=Bearer a=b=c"));
        assertEquals(Map.of("Authorization", "Bearer a=b=c"), cfg.headers());
    }

    @Test
    void http_transport_appends_metrics_path_to_bare_collector_endpoint() {
        var cfg = new OtlpMetricReporterConfig(Map.of(
                "otlp.metric.reporter.endpoint", "http://collector:4318",
                "otlp.metric.reporter.transport", "http"));
        assertEquals("http://collector:4318/v1/metrics", cfg.endpoint());
    }

    @Test
    void http_transport_keeps_signal_specific_metrics_endpoint() {
        var cfg = new OtlpMetricReporterConfig(Map.of(
                "otlp.metric.reporter.endpoint", "http://collector:4318/v1/metrics",
                "otlp.metric.reporter.transport", "http"));
        assertEquals("http://collector:4318/v1/metrics", cfg.endpoint());
    }

    @Test
    void throws_on_invalid_regex_pattern() {
        assertThrows(
                ConfigException.class,
                () -> new OtlpMetricReporterConfig(
                        Map.of("otlp.metric.reporter.allowed.metrics", "producer.[invalid")));
    }

    @Test
    void throws_on_invalid_endpoint() {
        assertThrows(
                ConfigException.class,
                () -> new OtlpMetricReporterConfig(Map.of("otlp.metric.reporter.endpoint", "not a uri")));
    }

    @Test
    void throws_on_unknown_transport() {
        assertThrows(
                ConfigException.class,
                () -> new OtlpMetricReporterConfig(Map.of("otlp.metric.reporter.transport", "udp")));
    }

    @Test
    void throws_on_malformed_resource_attributes() {
        assertThrows(
                ConfigException.class,
                () -> new OtlpMetricReporterConfig(Map.of("otlp.metric.reporter.resource.attributes", "noequals")));
    }

    @Test
    void throws_on_malformed_headers() {
        assertThrows(
                ConfigException.class,
                () -> new OtlpMetricReporterConfig(Map.of("otlp.metric.reporter.headers", "Authorization")));
        assertThrows(
                ConfigException.class,
                () -> new OtlpMetricReporterConfig(Map.of("otlp.metric.reporter.headers", "=secret")));
    }

    @Test
    void throws_on_unknown_compression() {
        assertThrows(
                ConfigException.class,
                () -> new OtlpMetricReporterConfig(Map.of("otlp.metric.reporter.compression", "brotli")));
    }

    @Test
    void throws_when_only_one_client_tls_path_is_configured() {
        assertThrows(
                ConfigException.class,
                () -> new OtlpMetricReporterConfig(
                        Map.of("otlp.metric.reporter.client.certificate.path", "/etc/ssl/client-cert.pem")));
        assertThrows(
                ConfigException.class,
                () -> new OtlpMetricReporterConfig(
                        Map.of("otlp.metric.reporter.client.key.path", "/etc/ssl/client-key.pem")));
    }

    @Test
    void throws_on_non_positive_interval_ms() {
        assertThrows(
                ConfigException.class,
                () -> new OtlpMetricReporterConfig(Map.of("otlp.metric.reporter.interval.ms", "0")));
        assertThrows(
                ConfigException.class,
                () -> new OtlpMetricReporterConfig(Map.of("otlp.metric.reporter.interval.ms", "-1")));
    }

    @Test
    void throws_on_non_positive_timeout_ms() {
        assertThrows(
                ConfigException.class,
                () -> new OtlpMetricReporterConfig(Map.of("otlp.metric.reporter.timeout.ms", "0")));
        assertThrows(
                ConfigException.class,
                () -> new OtlpMetricReporterConfig(Map.of("otlp.metric.reporter.timeout.ms", "-1")));
    }

    @Test
    void throws_when_timeout_exceeds_interval() {
        assertThrows(
                ConfigException.class,
                () -> new OtlpMetricReporterConfig(Map.of(
                        "otlp.metric.reporter.interval.ms", "1000",
                        "otlp.metric.reporter.timeout.ms", "1001")));
    }

    @Test
    void throws_when_timeout_equals_interval() {
        assertThrows(
                ConfigException.class,
                () -> new OtlpMetricReporterConfig(Map.of(
                        "otlp.metric.reporter.interval.ms", "1000",
                        "otlp.metric.reporter.timeout.ms", "1000")));
    }

    @Test
    void throws_on_unrecognised_boolean_value() {
        assertThrows(
                ConfigException.class,
                () -> new OtlpMetricReporterConfig(Map.of("otlp.metric.reporter.jvm.metrics.enabled", "tru")));
    }

    @Test
    void boolean_parsing_is_case_insensitive() {
        assertTrue(new OtlpMetricReporterConfig(Map.of("otlp.metric.reporter.jvm.metrics.enabled", "TRUE"))
                .jvmMetricsEnabled());
        assertFalse(new OtlpMetricReporterConfig(Map.of("otlp.metric.reporter.jvm.metrics.enabled", "False"))
                .jvmMetricsEnabled());
    }
}
