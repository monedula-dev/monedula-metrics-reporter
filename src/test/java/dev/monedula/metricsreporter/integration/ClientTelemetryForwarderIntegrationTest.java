// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter.integration;

import static org.junit.jupiter.api.Assertions.*;

import dev.monedula.metricsreporter.OtlpExporterFactory;
import dev.monedula.metricsreporter.OtlpMetricReporterConfig;
import dev.monedula.metricsreporter.clienttelemetry.ClientMetricsConverter;
import dev.monedula.metricsreporter.clienttelemetry.ClientMetricsEnricher;
import dev.monedula.metricsreporter.clienttelemetry.ClientTelemetryForwarder;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

@Testcontainers
class ClientTelemetryForwarderIntegrationTest {

    @Container
    static GenericContainer<?> collector = new GenericContainer<>("otel/opentelemetry-collector-contrib:0.152.0")
            .withExposedPorts(4317, 4318)
            .withCopyFileToContainer(
                    MountableFile.forClasspathResource("otel-collector-test-config.yaml"),
                    "/etc/otel-collector-config.yaml")
            .withCommand("--config=/etc/otel-collector-config.yaml")
            .waitingFor(Wait.forLogMessage(".*Everything is ready.*", 1));

    private static byte[] oneGauge() {
        return MetricsData.newBuilder()
                .addResourceMetrics(ResourceMetrics.newBuilder()
                        .addScopeMetrics(ScopeMetrics.newBuilder()
                                .setScope(InstrumentationScope.newBuilder()
                                        .setName("client")
                                        .build())
                                .addMetrics(Metric.newBuilder()
                                        .setName("client.count")
                                        .setGauge(Gauge.newBuilder()
                                                .addDataPoints(NumberDataPoint.newBuilder()
                                                        .setTimeUnixNano(1L)
                                                        .setAsDouble(1.0)
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build()
                .toByteArray();
    }

    private void assertCollectorLogsContain(String... expected) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            String logs = collector.getLogs();
            boolean allPresent = true;
            for (String s : expected) {
                if (!logs.contains(s)) {
                    allPresent = false;
                    break;
                }
            }
            if (allPresent) {
                return;
            }
            Thread.sleep(200);
        }
        fail("Collector logs did not contain all of "
                + java.util.Arrays.toString(expected)
                + "\n"
                + collector.getLogs());
    }

    @Test
    void forwards_client_telemetry_with_broker_identity_enrichment() throws InterruptedException {
        String endpoint = "http://" + collector.getHost() + ":" + collector.getMappedPort(4317);
        var cfg = new OtlpMetricReporterConfig(Map.of(
                "otlp.metric.reporter.endpoint", endpoint,
                "otlp.metric.reporter.transport", "grpc",
                // ClientTelemetryForwarder exports on-demand per submitted item, not on a timer, so this
                // interval has no effect here — it only satisfies config validation (timeout < interval).
                "otlp.metric.reporter.interval.ms", "60000",
                "otlp.metric.reporter.timeout.ms", "5000"));

        // Broker enrichment on, client-identity enrichment off: client-identity enrichment is intentionally
        // out of scope for this test (hence the null clientIdentity passed to submit below).
        var fwd = new ClientTelemetryForwarder(
                OtlpExporterFactory.create(cfg),
                new ClientMetricsConverter(new ClientMetricsEnricher(true, false, false, false)),
                16,
                5000L);
        fwd.setBrokerIdentity(Map.of("kafka.cluster.id", "C1"));
        fwd.start();
        try {
            fwd.submit(oneGauge(), null);
            assertCollectorLogsContain("client.count", "kafka.cluster.id: Str(C1)");
        } finally {
            fwd.stop();
        }
    }
}
