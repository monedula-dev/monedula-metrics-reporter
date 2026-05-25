// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class MetricCollectorTest {

    private KafkaMetric metric(String group, String name, double value) {
        KafkaMetric m = Mockito.mock(KafkaMetric.class);
        MetricName mn = new MetricName(name, group, "", Map.of());
        when(m.metricName()).thenReturn(mn);
        when(m.metricValue()).thenReturn(value);
        return m;
    }

    @Test
    void exporter_called_on_schedule() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        MetricExporter exporter = Mockito.mock(MetricExporter.class);
        when(exporter.export(any())).thenAnswer(inv -> {
            latch.countDown();
            return CompletableResultCode.ofSuccess();
        });
        when(exporter.shutdown()).thenReturn(CompletableResultCode.ofSuccess());

        MetricRegistry registry = new MetricRegistry(List.of());
        registry.update(metric("producer-metrics", "record-send-rate", 1.0));

        var mapper = new MetricDataMapper(Map.of());
        MetricCollector collector = new MetricCollector(registry, mapper, exporter, 100L, 1000L);
        collector.start();

        assertTrue(latch.await(2, TimeUnit.SECONDS), "exporter was not called within 2s");
        collector.stop();
    }

    @Test
    void export_failure_does_not_propagate() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        MetricExporter exporter = Mockito.mock(MetricExporter.class);
        when(exporter.export(any())).thenAnswer(inv -> {
            latch.countDown();
            return CompletableResultCode.ofFailure();
        });
        when(exporter.shutdown()).thenReturn(CompletableResultCode.ofSuccess());

        MetricRegistry registry = new MetricRegistry(List.of());
        registry.update(metric("producer-metrics", "record-send-rate", 1.0));

        var mapper = new MetricDataMapper(Map.of());
        MetricCollector collector = new MetricCollector(registry, mapper, exporter, 50L, 1000L);
        collector.start();

        assertTrue(latch.await(3, TimeUnit.SECONDS));
        assertDoesNotThrow(collector::stop);
    }

    @Test
    void empty_registry_still_exports_self_monitoring_metrics() throws InterruptedException {
        // Previously this test asserted no export when the registry was empty.
        // Self-monitoring metrics are emitted on every tick now (so operators see
        // success/failure counts even before any Kafka metric is registered), so
        // export() always runs - just with only the three self-metrics.
        CountDownLatch latch = new CountDownLatch(1);
        MetricExporter exporter = Mockito.mock(MetricExporter.class);
        ArgumentCaptor<Collection<MetricData>> batch = ArgumentCaptor.forClass(Collection.class);
        when(exporter.export(batch.capture())).thenAnswer(inv -> {
            latch.countDown();
            return CompletableResultCode.ofSuccess();
        });
        when(exporter.shutdown()).thenReturn(CompletableResultCode.ofSuccess());

        MetricCollector collector =
                new MetricCollector(new MetricRegistry(List.of()), new MetricDataMapper(Map.of()), exporter, 50L, 500L);
        collector.start();
        assertTrue(latch.await(2, TimeUnit.SECONDS), "exporter was not called within 2s");
        collector.stop();

        Set<String> names = batch.getValue().stream().map(MetricData::getName).collect(Collectors.toSet());
        assertEquals(
                Set.of(
                        "monedula_reporter_export_success",
                        "monedula_reporter_export_failure",
                        "monedula_reporter_export_duration_ms"),
                names,
                "empty-registry tick should contain exactly the three self-metrics");
    }

    @Test
    void success_counter_increments_on_each_successful_export() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(3);
        MetricExporter exporter = Mockito.mock(MetricExporter.class);
        when(exporter.export(any())).thenAnswer(inv -> {
            latch.countDown();
            return CompletableResultCode.ofSuccess();
        });
        when(exporter.shutdown()).thenReturn(CompletableResultCode.ofSuccess());

        MetricCollector collector =
                new MetricCollector(new MetricRegistry(List.of()), new MetricDataMapper(Map.of()), exporter, 50L, 500L);
        collector.start();
        assertTrue(latch.await(3, TimeUnit.SECONDS), "expected three exports within 3s");
        collector.stop();

        // The counter should have ticked up at least three times; depending on
        // interleaving with stop() it may have ticked one or two more.
        assertTrue(
                collector.exportSuccessCount() >= 3,
                "success count was " + collector.exportSuccessCount() + ", expected >= 3");
        assertEquals(0, collector.exportFailureCount(), "no failures expected, got " + collector.exportFailureCount());
    }

    @Test
    void failure_counter_increments_on_each_failed_export() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(3);
        MetricExporter exporter = Mockito.mock(MetricExporter.class);
        when(exporter.export(any())).thenAnswer(inv -> {
            latch.countDown();
            return CompletableResultCode.ofFailure();
        });
        when(exporter.shutdown()).thenReturn(CompletableResultCode.ofSuccess());

        MetricCollector collector =
                new MetricCollector(new MetricRegistry(List.of()), new MetricDataMapper(Map.of()), exporter, 50L, 500L);
        collector.start();
        assertTrue(latch.await(3, TimeUnit.SECONDS), "expected three failed export attempts within 3s");
        collector.stop();

        assertTrue(
                collector.exportFailureCount() >= 3,
                "failure count was " + collector.exportFailureCount() + ", expected >= 3");
        assertEquals(0, collector.exportSuccessCount(), "no successes expected, got " + collector.exportSuccessCount());
    }
}
