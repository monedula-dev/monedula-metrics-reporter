// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
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

        MetricRegistry registry = new MetricRegistry();
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

        MetricRegistry registry = new MetricRegistry();
        registry.update(metric("producer-metrics", "record-send-rate", 1.0));

        var mapper = new MetricDataMapper(Map.of());
        MetricCollector collector = new MetricCollector(registry, mapper, exporter, 50L, 1000L);
        collector.start();

        assertTrue(latch.await(3, TimeUnit.SECONDS));
        assertDoesNotThrow(collector::stop);
    }

    @Test
    void error_thrown_during_export_does_not_kill_the_scheduler() throws InterruptedException {
        // A Throwable that isn't an Exception (e.g. a lazily-triggered NoClassDefFoundError on
        // the first export, or an AssertionError) must not cancel the fixed-rate schedule.
        // scheduleAtFixedRate silently suppresses all future runs if a task throws, so the
        // export thread would go dark with no log — violating the fail-open assumption.
        CountDownLatch latch = new CountDownLatch(3);
        MetricExporter exporter = Mockito.mock(MetricExporter.class);
        when(exporter.export(any())).thenAnswer(inv -> {
            latch.countDown();
            throw new AssertionError("boom"); // an Error, not an Exception
        });
        when(exporter.shutdown()).thenReturn(CompletableResultCode.ofSuccess());

        MetricCollector collector =
                new MetricCollector(new MetricRegistry(), new MetricDataMapper(Map.of()), exporter, 50L, 500L);
        collector.start();
        assertTrue(
                latch.await(3, TimeUnit.SECONDS), "scheduler stopped after an Error in a tick; export was not retried");
        collector.stop();
        assertTrue(
                collector.exportFailureCount() >= 3,
                "failure count was " + collector.exportFailureCount() + ", expected >= 3");
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
                new MetricCollector(new MetricRegistry(), new MetricDataMapper(Map.of()), exporter, 50L, 500L);
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
                new MetricCollector(new MetricRegistry(), new MetricDataMapper(Map.of()), exporter, 50L, 500L);
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
                new MetricCollector(new MetricRegistry(), new MetricDataMapper(Map.of()), exporter, 50L, 500L);
        collector.start();
        assertTrue(latch.await(3, TimeUnit.SECONDS), "expected three failed export attempts within 3s");
        collector.stop();

        assertTrue(
                collector.exportFailureCount() >= 3,
                "failure count was " + collector.exportFailureCount() + ", expected >= 3");
        assertEquals(0, collector.exportSuccessCount(), "no successes expected, got " + collector.exportSuccessCount());
    }

    @Test
    void emits_client_telemetry_self_metrics_when_counters_present() {
        ArgumentCaptor<Collection<MetricData>> batch = ArgumentCaptor.forClass(Collection.class);
        MetricExporter exporter = Mockito.mock(MetricExporter.class);
        when(exporter.export(batch.capture())).thenReturn(CompletableResultCode.ofSuccess());
        when(exporter.shutdown()).thenReturn(CompletableResultCode.ofSuccess());

        // long interval so the scheduler won't fire on its own; we drive a single tick via exportOnceForTest()
        MetricCollector collector =
                new MetricCollector(new MetricRegistry(), new MetricDataMapper(Map.of()), exporter, 60_000L, 1_000L);
        // Distinct value per counter so a transposed name-to-counter mapping fails the assertions below.
        collector.setClientTelemetryCounters(() -> new MetricCollector.ClientTelemetryCounters(7L, 5L, 2L, 1L, 3L));
        try {
            collector.exportOnceForTest();
            Collection<MetricData> metrics = batch.getValue();
            assertEquals(7.0, selfMetricValue(metrics, "monedula_reporter_clienttelemetry_received_total"));
            assertEquals(5.0, selfMetricValue(metrics, "monedula_reporter_clienttelemetry_forwarded_total"));
            assertEquals(2.0, selfMetricValue(metrics, "monedula_reporter_clienttelemetry_dropped_total"));
            assertEquals(
                    1.0,
                    selfMetricValue(metrics, "monedula_reporter_clienttelemetry_unsupported_metrics_dropped_total"));
            assertEquals(3.0, selfMetricValue(metrics, "monedula_reporter_clienttelemetry_queue_depth"));
        } finally {
            collector.stop();
        }
    }

    @Test
    void allow_list_excludes_non_matching_spi_metric_from_export() {
        ArgumentCaptor<Collection<MetricData>> batch = ArgumentCaptor.forClass(Collection.class);
        MetricExporter exporter = Mockito.mock(MetricExporter.class);
        when(exporter.export(batch.capture())).thenReturn(CompletableResultCode.ofSuccess());
        when(exporter.shutdown()).thenReturn(CompletableResultCode.ofSuccess());

        MetricRegistry registry = new MetricRegistry();
        registry.update(metric("producer-metrics", "record-send-rate", 1.0));
        registry.update(metric("consumer-metrics", "records-consumed-rate", 2.0));

        MetricCollector collector =
                new MetricCollector(registry, new MetricDataMapper(Map.of()), exporter, 60_000L, 1_000L);
        collector.replaceAllowList(new AllowList(List.of(Pattern.compile("producer-metrics\\..*"))));
        try {
            collector.exportOnceForTest();
            Set<String> names = exportedNames(batch.getValue());
            assertTrue(names.contains("producer_metrics_record_send_rate"), "allowed metric should be present");
            assertFalse(names.contains("consumer_metrics_records_consumed_rate"), "excluded metric should be absent");
        } finally {
            collector.stop();
        }
    }

    @Test
    void widening_the_allow_list_resurrects_a_previously_filtered_metric() {
        // The scenario ingest-time filtering could never support: a metric stored in the
        // registry but filtered out at export appears once the allow-list is widened — no
        // re-registration from Kafka required.
        ArgumentCaptor<Collection<MetricData>> batch = ArgumentCaptor.forClass(Collection.class);
        MetricExporter exporter = Mockito.mock(MetricExporter.class);
        when(exporter.export(batch.capture())).thenReturn(CompletableResultCode.ofSuccess());
        when(exporter.shutdown()).thenReturn(CompletableResultCode.ofSuccess());

        MetricRegistry registry = new MetricRegistry();
        registry.update(metric("producer-metrics", "record-send-rate", 1.0));
        registry.update(metric("consumer-metrics", "records-consumed-rate", 2.0));

        MetricCollector collector =
                new MetricCollector(registry, new MetricDataMapper(Map.of()), exporter, 60_000L, 1_000L);
        collector.replaceAllowList(new AllowList(List.of(Pattern.compile("producer-metrics\\..*"))));
        try {
            collector.exportOnceForTest();
            assertFalse(
                    exportedNames(batch.getValue()).contains("consumer_metrics_records_consumed_rate"),
                    "consumer metric should be filtered out under the narrow list");

            // Widen to include consumer metrics — the stored-but-filtered metric now surfaces.
            collector.replaceAllowList(
                    new AllowList(List.of(Pattern.compile("producer-metrics\\..*"), Pattern.compile("consumer.*"))));
            collector.exportOnceForTest();
            Set<String> names = exportedNames(batch.getValue());
            assertTrue(
                    names.contains("consumer_metrics_records_consumed_rate"),
                    "widened list should resurrect the previously-filtered metric");
            assertTrue(names.contains("producer_metrics_record_send_rate"), "producer metric still present");
        } finally {
            collector.stop();
        }
    }

    @Test
    void narrowing_the_allow_list_drops_a_previously_exported_metric() {
        ArgumentCaptor<Collection<MetricData>> batch = ArgumentCaptor.forClass(Collection.class);
        MetricExporter exporter = Mockito.mock(MetricExporter.class);
        when(exporter.export(batch.capture())).thenReturn(CompletableResultCode.ofSuccess());
        when(exporter.shutdown()).thenReturn(CompletableResultCode.ofSuccess());

        MetricRegistry registry = new MetricRegistry();
        registry.update(metric("producer-metrics", "record-send-rate", 1.0));
        registry.update(metric("consumer-metrics", "records-consumed-rate", 2.0));

        MetricCollector collector =
                new MetricCollector(registry, new MetricDataMapper(Map.of()), exporter, 60_000L, 1_000L);
        // Default ALLOW_ALL: both present initially.
        try {
            collector.exportOnceForTest();
            Set<String> before = exportedNames(batch.getValue());
            assertTrue(before.contains("producer_metrics_record_send_rate"));
            assertTrue(before.contains("consumer_metrics_records_consumed_rate"));

            collector.replaceAllowList(new AllowList(List.of(Pattern.compile("producer-metrics\\..*"))));
            collector.exportOnceForTest();
            Set<String> after = exportedNames(batch.getValue());
            assertTrue(after.contains("producer_metrics_record_send_rate"), "producer metric still present");
            assertFalse(after.contains("consumer_metrics_records_consumed_rate"), "narrowed list should drop consumer");
        } finally {
            collector.stop();
        }
    }

    @Test
    void allow_list_filters_yammer_metrics_on_group_type_name_subject() {
        ArgumentCaptor<Collection<MetricData>> batch = ArgumentCaptor.forClass(Collection.class);
        MetricExporter exporter = Mockito.mock(MetricExporter.class);
        when(exporter.export(batch.capture())).thenReturn(CompletableResultCode.ofSuccess());
        when(exporter.shutdown()).thenReturn(CompletableResultCode.ofSuccess());

        com.yammer.metrics.core.MetricsRegistry yammer = new com.yammer.metrics.core.MetricsRegistry();
        yammer.newCounter(
                new com.yammer.metrics.core.MetricName("kafka.server", "ReplicaManager", "UnderReplicatedPartitions"));
        yammer.newCounter(new com.yammer.metrics.core.MetricName("kafka.network", "RequestMetrics", "RequestsPerSec"));
        var yr = new dev.monedula.metricsreporter.yammer.YammerMetricRegistry();
        yr.attach(yammer);

        MetricCollector collector =
                new MetricCollector(new MetricRegistry(), new MetricDataMapper(Map.of()), exporter, 60_000L, 1_000L);
        collector.setYammer(yr, new dev.monedula.metricsreporter.yammer.YammerMetricDataMapper(Map.of()));
        // Subject is {group}.{type}.{name}; only the kafka.server metric matches.
        collector.replaceAllowList(new AllowList(List.of(Pattern.compile("kafka\\.server\\..*"))));
        try {
            collector.exportOnceForTest();
            Set<String> names = exportedNames(batch.getValue());
            assertTrue(
                    names.contains("kafka_server_replicamanager_underreplicatedpartitions"),
                    "kafka.server Yammer metric should pass the filter");
            assertFalse(
                    names.contains("kafka_network_requestmetrics_requestspersec"),
                    "kafka.network Yammer metric should be filtered out");
        } finally {
            collector.stop();
        }
    }

    /** Names of all metrics in the exported batch. */
    private static Set<String> exportedNames(Collection<MetricData> batch) {
        return batch.stream().map(MetricData::getName).collect(Collectors.toSet());
    }

    /** Value of the single data point of the named self-metric (sum or gauge) in the exported batch. */
    private static double selfMetricValue(Collection<MetricData> batch, String name) {
        MetricData md = batch.stream()
                .filter(m -> m.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("metric not exported: " + name));
        var points = md.getType() == MetricDataType.DOUBLE_SUM
                ? md.getDoubleSumData().getPoints()
                : md.getDoubleGaugeData().getPoints();
        return points.iterator().next().getValue();
    }
}
