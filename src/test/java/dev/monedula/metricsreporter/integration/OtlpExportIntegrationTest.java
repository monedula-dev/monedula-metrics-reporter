// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter.integration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import dev.monedula.metricsreporter.*;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

@Testcontainers
class OtlpExportIntegrationTest {

    @Container
    static GenericContainer<?> collector = new GenericContainer<>("otel/opentelemetry-collector-contrib:0.152.0")
            .withExposedPorts(4317, 4318)
            .withCopyFileToContainer(
                    MountableFile.forClasspathResource("otel-collector-test-config.yaml"),
                    "/etc/otel-collector-config.yaml")
            .withCommand("--config=/etc/otel-collector-config.yaml")
            .waitingFor(Wait.forLogMessage(".*Everything is ready.*", 1));

    private KafkaMetric metric(String group, String name, double value) {
        KafkaMetric m = Mockito.mock(KafkaMetric.class);
        when(m.metricName()).thenReturn(new MetricName(name, group, "desc", Map.of("client-id", "test")));
        when(m.metricValue()).thenReturn(value);
        return m;
    }

    private void assertCollectorLogsContain(String expected) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            if (collector.getLogs().contains(expected)) {
                return;
            }
            Thread.sleep(200);
        }
        fail("Collector logs did not contain " + expected + "\n" + collector.getLogs());
    }

    @Test
    void exports_metrics_via_grpc() throws InterruptedException {
        String endpoint = "http://" + collector.getHost() + ":" + collector.getMappedPort(4317);
        String metricName = "producer_metrics_record_send_rate_grpc";
        OtlpMetricReporter reporter = new OtlpMetricReporter();
        reporter.configure(Map.of(
                "otlp.metric.reporter.endpoint", endpoint,
                "otlp.metric.reporter.transport", "grpc",
                "otlp.metric.reporter.interval.ms", "500",
                "otlp.metric.reporter.timeout.ms", "250",
                "otlp.metric.reporter.jvm.metrics.enabled", "false"));
        reporter.init(List.of(metric("producer-metrics", "record-send-rate-grpc", 42.0)));
        try {
            assertCollectorLogsContain(metricName);
        } finally {
            reporter.close();
        }
    }

    @Test
    void exports_metrics_via_http() throws InterruptedException {
        String endpoint = "http://" + collector.getHost() + ":" + collector.getMappedPort(4318);
        String metricName = "producer_metrics_record_send_rate_http";
        OtlpMetricReporter reporter = new OtlpMetricReporter();
        reporter.configure(Map.of(
                "otlp.metric.reporter.endpoint", endpoint,
                "otlp.metric.reporter.transport", "http",
                "otlp.metric.reporter.interval.ms", "500",
                "otlp.metric.reporter.timeout.ms", "250",
                "otlp.metric.reporter.jvm.metrics.enabled", "false"));
        reporter.init(List.of(metric("producer-metrics", "record-send-rate-http", 99.0)));
        try {
            assertCollectorLogsContain(metricName);
        } finally {
            reporter.close();
        }
    }

    @Test
    void collector_unavailable_does_not_throw() {
        OtlpMetricReporter reporter = new OtlpMetricReporter();
        reporter.configure(Map.of(
                "otlp.metric.reporter.endpoint", "http://127.0.0.1:19999",
                "otlp.metric.reporter.interval.ms", "200",
                "otlp.metric.reporter.timeout.ms", "100",
                "otlp.metric.reporter.jvm.metrics.enabled", "false"));
        reporter.init(List.of(metric("producer-metrics", "record-send-rate", 1.0)));
        assertDoesNotThrow(() -> {
            Thread.sleep(700);
            reporter.close();
        });
    }
}
