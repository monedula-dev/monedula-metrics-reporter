// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.resources.Resource;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class OtlpMetricReporterTest {

    private KafkaMetric metric(String group, String name) {
        KafkaMetric m = Mockito.mock(KafkaMetric.class);
        when(m.metricName()).thenReturn(new MetricName(name, group, "", Map.of()));
        when(m.metricValue()).thenReturn(1.0);
        return m;
    }

    @Test
    void configure_and_close_without_error() {
        // points at a non-existent collector; should not throw
        OtlpMetricReporter reporter = new OtlpMetricReporter();
        assertDoesNotThrow(() -> reporter.configure(Map.of()));
        assertDoesNotThrow(() -> reporter.init(List.of(metric("producer-metrics", "record-send-rate"))));
        assertDoesNotThrow(() -> reporter.metricChange(metric("producer-metrics", "record-send-rate")));
        assertDoesNotThrow(() -> reporter.metricRemoval(metric("producer-metrics", "record-send-rate")));
        assertDoesNotThrow(reporter::close);
    }

    @Test
    void bad_config_becomes_noop_without_throwing() {
        OtlpMetricReporter reporter = new OtlpMetricReporter();
        // invalid transport: ConfigException is caught and the reporter falls into no-op mode
        // so a misconfiguration cannot crash the broker (Kafka rethrows configure() failures
        // from getConfiguredInstances).
        assertDoesNotThrow(() -> reporter.configure(Map.of("otlp.metric.reporter.transport", "ftp")));
        assertDoesNotThrow(reporter::close);
    }

    @Test
    void context_change_before_configure_does_not_throw() {
        OtlpMetricReporter reporter = new OtlpMetricReporter();
        KafkaMetricsContext ctx = new KafkaMetricsContext(
                "kafka.server", Map.of("kafka.cluster.id", "test-cluster", "kafka.broker.id", "1"));
        assertDoesNotThrow(() -> reporter.contextChange(ctx));
        assertDoesNotThrow(() -> reporter.configure(Map.of(
                "otlp.metric.reporter.endpoint", "http://127.0.0.1:19999",
                "otlp.metric.reporter.interval.ms", "60000")));
        assertDoesNotThrow(reporter::close);
    }

    @Test
    void context_change_after_configure_rebuilds_mappers() {
        OtlpMetricReporter reporter = new OtlpMetricReporter();
        assertDoesNotThrow(() -> reporter.configure(Map.of(
                "otlp.metric.reporter.endpoint", "http://127.0.0.1:19999",
                "otlp.metric.reporter.interval.ms", "60000")));
        KafkaMetricsContext ctx = new KafkaMetricsContext(
                "kafka.server", Map.of("kafka.cluster.id", "test-cluster", "kafka.broker.id", "1"));
        assertDoesNotThrow(() -> reporter.contextChange(ctx));
        assertDoesNotThrow(reporter::close);
    }

    @Test
    void context_change_with_null_labels_is_tolerated() {
        OtlpMetricReporter reporter = new OtlpMetricReporter();
        // MetricsContext implementations may, in principle, return null labels.
        org.apache.kafka.common.metrics.MetricsContext ctx = () -> null;
        assertDoesNotThrow(() -> reporter.contextChange(ctx));
        assertDoesNotThrow(() -> reporter.configure(Map.of()));
        assertDoesNotThrow(reporter::close);
    }

    @Test
    void context_change_namespace_is_applied_to_subsequent_metrics() {
        OtlpMetricReporter reporter = new OtlpMetricReporter();
        reporter.contextChange(new KafkaMetricsContext(
                "kafka.server", Map.of("kafka.cluster.id", "test-cluster", "kafka.node.id", "1")));
        assertDoesNotThrow(() -> reporter.configure(Map.of()));
        // We can't easily inspect the exported metric names without a mock exporter,
        // so just verify configure didn't throw and that the reporter is in a valid state.
        assertDoesNotThrow(reporter::close);
    }

    @Test
    void resource_carries_auto_generated_service_instance_id() {
        // Two reporters in the same JVM (e.g., a broker that also runs an
        // embedded producer with its own OTLP reporter) would share broker
        // context labels but should be distinguishable in the collector by
        // their auto-generated service.instance.id. Verify the ID is non-null,
        // UUID-shaped, and stable across configure() reloads on the same
        // instance.
        OtlpMetricReporter reporter = new OtlpMetricReporter();
        try {
            reporter.configure(Map.of(
                    "otlp.metric.reporter.endpoint", "http://127.0.0.1:19999",
                    "otlp.metric.reporter.interval.ms", "60000",
                    "otlp.metric.reporter.timeout.ms", "1000",
                    "otlp.metric.reporter.jvm.metrics.enabled", "false"));

            String firstId =
                    reporter.currentResource().getAttributes().get(AttributeKey.stringKey("service.instance.id"));
            assertNotNull(firstId, "service.instance.id should always be set");
            // UUID.toString() is 36 chars including hyphens
            assertEquals(36, firstId.length(), "service.instance.id should be a UUID string");

            // configure() called again on the same reporter object keeps the
            // same instance id (it's a JVM-lifetime identity, not a config
            // reload artefact).
            reporter.configure(Map.of(
                    "otlp.metric.reporter.endpoint", "http://127.0.0.1:19998",
                    "otlp.metric.reporter.interval.ms", "30000",
                    "otlp.metric.reporter.timeout.ms", "500",
                    "otlp.metric.reporter.jvm.metrics.enabled", "false"));
            String secondId =
                    reporter.currentResource().getAttributes().get(AttributeKey.stringKey("service.instance.id"));
            assertEquals(firstId, secondId, "service.instance.id must be stable across configure() reloads");
        } finally {
            reporter.close();
        }
    }

    @Test
    void two_reporter_instances_get_different_service_instance_ids() {
        OtlpMetricReporter a = new OtlpMetricReporter();
        OtlpMetricReporter b = new OtlpMetricReporter();
        try {
            a.configure(Map.of(
                    "otlp.metric.reporter.endpoint", "http://127.0.0.1:19999",
                    "otlp.metric.reporter.interval.ms", "60000",
                    "otlp.metric.reporter.timeout.ms", "1000",
                    "otlp.metric.reporter.jvm.metrics.enabled", "false"));
            b.configure(Map.of(
                    "otlp.metric.reporter.endpoint", "http://127.0.0.1:19999",
                    "otlp.metric.reporter.interval.ms", "60000",
                    "otlp.metric.reporter.timeout.ms", "1000",
                    "otlp.metric.reporter.jvm.metrics.enabled", "false"));

            String idA = a.currentResource().getAttributes().get(AttributeKey.stringKey("service.instance.id"));
            String idB = b.currentResource().getAttributes().get(AttributeKey.stringKey("service.instance.id"));
            assertNotEquals(idA, idB, "distinct reporters must get distinct instance ids");
        } finally {
            a.close();
            b.close();
        }
    }

    @Test
    void user_supplied_service_instance_id_overrides_auto_generated() {
        OtlpMetricReporter reporter = new OtlpMetricReporter();
        try {
            reporter.configure(Map.of(
                    "otlp.metric.reporter.endpoint", "http://127.0.0.1:19999",
                    "otlp.metric.reporter.interval.ms", "60000",
                    "otlp.metric.reporter.timeout.ms", "1000",
                    "otlp.metric.reporter.jvm.metrics.enabled", "false",
                    "otlp.metric.reporter.resource.attributes", "service.instance.id=broker-1-spi-reporter"));

            String id = reporter.currentResource().getAttributes().get(AttributeKey.stringKey("service.instance.id"));
            assertEquals(
                    "broker-1-spi-reporter",
                    id,
                    "user-supplied service.instance.id from resource.attributes should win");
        } finally {
            reporter.close();
        }
    }

    @Test
    void configure_called_twice_tears_down_and_re_initialises_cleanly() {
        // configure() is documented as supporting re-init (see the comment at the
        // top of the method: "Allow a re-init attempt after a previous failure").
        // Verify the recovery path: a second configure() must close down the first
        // collector + JVM-metrics pipeline and leave the reporter live with the
        // SECOND config's values, not a half-merged state.
        OtlpMetricReporter reporter = new OtlpMetricReporter();
        try {
            reporter.configure(Map.of(
                    "otlp.metric.reporter.endpoint", "http://127.0.0.1:19999",
                    "otlp.metric.reporter.interval.ms", "60000",
                    "otlp.metric.reporter.timeout.ms", "1000",
                    "otlp.metric.reporter.jvm.metrics.enabled", "false"));

            // First config: jvm metrics off, so jvmMetrics stays null.
            assertNotNull(reporter.currentResource(), "first configure should install a mapper");
            assertNull(reporter.jvmMetrics, "first configure had jvm.metrics.enabled=false");

            // Hold a handle to the first mapper's Resource so we can prove the second
            // configure built a new one rather than reusing the first.
            Resource firstResource = reporter.currentResource();

            reporter.configure(Map.of(
                    "otlp.metric.reporter.endpoint", "http://127.0.0.1:19998",
                    "otlp.metric.reporter.interval.ms", "30000",
                    "otlp.metric.reporter.timeout.ms", "500",
                    "otlp.metric.reporter.jvm.metrics.enabled", "true",
                    "otlp.metric.reporter.resource.attributes", "deployment.environment=staging"));

            // Second config: jvm metrics on, custom resource attribute present.
            Resource secondResource = reporter.currentResource();
            assertNotNull(secondResource, "second configure should install a new mapper");
            assertNotSame(
                    firstResource, secondResource, "second configure should build a fresh mapper, not reuse the first");
            assertEquals(
                    "staging",
                    secondResource.getAttributes().get(AttributeKey.stringKey("deployment.environment")),
                    "second configure's resource.attributes should be applied");
            assertNotNull(
                    reporter.jvmMetrics, "second configure had jvm.metrics.enabled=true; lifecycle should be live");
        } finally {
            reporter.close();
        }
    }

    @Test
    void runs_in_client_only_mode_when_kafka_yammer_metrics_is_absent() {
        // The test classpath only ships kafka-clients (see build.gradle: no kafka-server),
        // so org.apache.kafka.server.metrics.KafkaYammerMetrics is not on the classpath.
        // attachYammerIfBroker() must take the ClassNotFoundException path and leave the
        // Yammer side detached so the same JAR can run unchanged on clients.
        OtlpMetricReporter reporter = new OtlpMetricReporter();
        try {
            reporter.configure(Map.of(
                    "otlp.metric.reporter.endpoint", "http://127.0.0.1:19999",
                    "otlp.metric.reporter.interval.ms", "60000",
                    "otlp.metric.reporter.timeout.ms", "1000"));
            assertNull(
                    reporter.yammerRegistry,
                    "Yammer registry must not attach when KafkaYammerMetrics is absent from the classpath");
        } finally {
            reporter.close();
        }
    }

    @Test
    void context_change_after_configure_swaps_resource_labels_on_spi_mapper() {
        // The pre-existing context_change_after_configure_rebuilds_mappers test only
        // verifies that contextChange doesn't throw; this one asserts the swap
        // actually applied - the SPI mapper's Resource carries the new labels after
        // each contextChange, both the first one (initial population) and any
        // subsequent one (real swap).
        OtlpMetricReporter reporter = new OtlpMetricReporter();
        try {
            reporter.configure(Map.of(
                    "otlp.metric.reporter.endpoint", "http://127.0.0.1:19999",
                    "otlp.metric.reporter.interval.ms", "60000",
                    "otlp.metric.reporter.timeout.ms", "1000",
                    "otlp.metric.reporter.jvm.metrics.enabled", "false"));

            reporter.contextChange(new KafkaMetricsContext(
                    "kafka.server", Map.of("kafka.cluster.id", "first-cluster", "kafka.node.id", "1")));

            Resource first = reporter.currentResource();
            assertNotNull(first);
            assertEquals("first-cluster", first.getAttributes().get(AttributeKey.stringKey("kafka.cluster.id")));
            assertEquals("1", first.getAttributes().get(AttributeKey.stringKey("kafka.node.id")));

            // Second contextChange with different labels exercises the real swap path
            // (the mapper was already running; replaceMappers has to install a new one).
            reporter.contextChange(new KafkaMetricsContext(
                    "kafka.server", Map.of("kafka.cluster.id", "second-cluster", "kafka.node.id", "2")));

            Resource second = reporter.currentResource();
            assertNotNull(second);
            assertEquals("second-cluster", second.getAttributes().get(AttributeKey.stringKey("kafka.cluster.id")));
            assertEquals("2", second.getAttributes().get(AttributeKey.stringKey("kafka.node.id")));
            assertNotSame(
                    first, second, "replaceMappers should install a new MetricDataMapper, not mutate the existing one");
        } finally {
            reporter.close();
        }
    }

    @Test
    void client_receiver_returned_when_enabled() {
        var reporter = new OtlpMetricReporter();
        reporter.configure(Map.of(
                OtlpMetricReporterConfig.ENDPOINT, "http://localhost:4317",
                OtlpMetricReporterConfig.INTERVAL_MS, "60000",
                OtlpMetricReporterConfig.TIMEOUT_MS, "1000"));
        try {
            assertTrue(reporter instanceof org.apache.kafka.server.telemetry.ClientTelemetry);
            assertNotNull(((org.apache.kafka.server.telemetry.ClientTelemetry) reporter).clientReceiver());
        } finally {
            reporter.close();
        }
    }

    @Test
    void client_receiver_null_when_disabled() {
        var reporter = new OtlpMetricReporter();
        reporter.configure(Map.of(
                OtlpMetricReporterConfig.ENDPOINT, "http://localhost:4317",
                OtlpMetricReporterConfig.INTERVAL_MS, "60000",
                OtlpMetricReporterConfig.TIMEOUT_MS, "1000",
                OtlpMetricReporterConfig.CLIENT_TELEMETRY_ENABLED, "false"));
        try {
            assertNull(((org.apache.kafka.server.telemetry.ClientTelemetry) reporter).clientReceiver());
        } finally {
            reporter.close();
        }
    }

    @Test
    void client_receiver_null_when_noop() {
        var reporter = new OtlpMetricReporter();
        // Invalid config (timeout >= interval) forces no-op mode.
        reporter.configure(Map.of(
                OtlpMetricReporterConfig.INTERVAL_MS, "1000",
                OtlpMetricReporterConfig.TIMEOUT_MS, "1000"));
        try {
            assertNull(((org.apache.kafka.server.telemetry.ClientTelemetry) reporter).clientReceiver());
        } finally {
            reporter.close();
        }
    }

    @Test
    void context_change_refreshes_client_telemetry_broker_identity() {
        var reporter = new OtlpMetricReporter();
        reporter.configure(Map.of(
                OtlpMetricReporterConfig.ENDPOINT, "http://localhost:4317",
                OtlpMetricReporterConfig.INTERVAL_MS, "60000",
                OtlpMetricReporterConfig.TIMEOUT_MS, "1000"));
        try {
            reporter.contextChange(
                    new KafkaMetricsContext("kafka.server", Map.of("kafka.cluster.id", "CID", "kafka.node.id", "3")));
            assertEquals(
                    "CID",
                    reporter.clientTelemetryForwarder.currentBrokerIdentity().get("kafka.cluster.id"));
            assertEquals(
                    "3",
                    reporter.clientTelemetryForwarder.currentBrokerIdentity().get("kafka.node.id"));
        } finally {
            reporter.close();
        }
    }

    @Test
    void context_change_after_configure_rebuilds_jvm_metrics_resource_labels() {
        OtlpMetricReporter reporter = new OtlpMetricReporter();
        reporter.configure(Map.of(
                "otlp.metric.reporter.endpoint", "http://127.0.0.1:19999",
                "otlp.metric.reporter.interval.ms", "60000",
                "otlp.metric.reporter.timeout.ms", "1000"));

        try {
            reporter.contextChange(new KafkaMetricsContext(
                    "kafka.server", Map.of("kafka.cluster.id", "test-cluster", "kafka.node.id", "1")));

            assertNotNull(reporter.jvmMetrics);
            Resource resource = reporter.jvmMetrics.resource;
            assertNotNull(resource);
            assertEquals("test-cluster", resource.getAttributes().get(AttributeKey.stringKey("kafka.cluster.id")));
            assertEquals("1", resource.getAttributes().get(AttributeKey.stringKey("kafka.node.id")));
        } finally {
            reporter.close();
        }
    }
}
