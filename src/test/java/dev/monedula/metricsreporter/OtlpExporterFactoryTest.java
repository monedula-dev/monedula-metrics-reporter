// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter;

import static org.junit.jupiter.api.Assertions.*;

import io.opentelemetry.sdk.metrics.export.MetricExporter;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OtlpExporterFactoryTest {

    @Test
    void builds_grpc_exporter() {
        var cfg = new OtlpMetricReporterConfig(Map.of("otlp.metric.reporter.transport", "grpc"));
        MetricExporter exporter = OtlpExporterFactory.create(cfg);
        assertNotNull(exporter);
        exporter.shutdown();
    }

    @Test
    void builds_http_exporter() {
        var cfg = new OtlpMetricReporterConfig(Map.of("otlp.metric.reporter.transport", "http"));
        MetricExporter exporter = OtlpExporterFactory.create(cfg);
        assertNotNull(exporter);
        exporter.shutdown();
    }

    @Test
    void builds_exporter_with_headers_and_gzip_compression() {
        var cfg = new OtlpMetricReporterConfig(Map.of(
                "otlp.metric.reporter.transport", "http",
                "otlp.metric.reporter.headers", "Authorization=Bearer test-token",
                "otlp.metric.reporter.compression", "gzip"));
        assertEquals(Map.of("Authorization", "Bearer test-token"), cfg.headers());
        assertEquals("gzip", cfg.compression());

        MetricExporter exporter = OtlpExporterFactory.create(cfg);
        assertNotNull(exporter);
        exporter.shutdown();
    }
}
