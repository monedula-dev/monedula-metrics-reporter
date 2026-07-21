// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class MetricRegistryTest {

    private KafkaMetric metric(String group, String name, Map<String, String> tags) {
        KafkaMetric m = Mockito.mock(KafkaMetric.class);
        MetricName mn = new MetricName(name, group, "", tags);
        when(m.metricName()).thenReturn(mn);
        when(m.metricValue()).thenReturn(1.0);
        return m;
    }

    @Test
    void metric_is_stored() {
        MetricRegistry registry = new MetricRegistry();
        registry.update(metric("producer-metrics", "record-send-rate", Map.of()));
        assertEquals(1, registry.snapshot().size());
    }

    @Test
    void every_metric_is_stored_regardless_of_name() {
        // The registry no longer filters; the allow-list is applied at export time by the
        // collector. Even a metric that would match no configured pattern is stored here so a
        // later live widening of the allow-list can resurrect it.
        MetricRegistry registry = new MetricRegistry();
        registry.update(metric("producer-metrics", "record-send-rate", Map.of()));
        registry.update(metric("consumer-metrics", "records-consumed-rate", Map.of()));
        registry.update(metric("kafka.server", "some-obscure-metric", Map.of()));
        assertEquals(3, registry.snapshot().size());
    }

    @Test
    void metric_is_removed() {
        MetricRegistry registry = new MetricRegistry();
        KafkaMetric m = metric("producer-metrics", "record-send-rate", Map.of());
        registry.update(m);
        registry.remove(m);
        assertEquals(0, registry.snapshot().size());
    }

    @Test
    void snapshot_returns_independent_copy() {
        MetricRegistry registry = new MetricRegistry();
        KafkaMetric m = metric("producer-metrics", "record-send-rate", Map.of());
        registry.update(m);
        var snap = registry.snapshot();
        registry.remove(m);
        assertEquals(1, snap.size()); // snapshot not affected by subsequent remove
    }

    @Test
    void nan_metric_value_is_excluded_from_snapshot() {
        MetricRegistry registry = new MetricRegistry();
        KafkaMetric m = metric("producer-metrics", "record-send-rate", Map.of());
        when(m.metricValue()).thenReturn(Double.NaN);
        registry.update(m);
        assertEquals(0, registry.snapshot().size());
    }

    @Test
    void infinite_metric_value_is_excluded_from_snapshot() {
        MetricRegistry registry = new MetricRegistry();
        KafkaMetric m = metric("producer-metrics", "record-send-rate", Map.of());
        when(m.metricValue()).thenReturn(Double.POSITIVE_INFINITY);
        registry.update(m);
        assertEquals(0, registry.snapshot().size());
    }

    @Test
    void long_metric_value_is_included_in_snapshot() {
        MetricRegistry registry = new MetricRegistry();
        KafkaMetric m = metric("producer-metrics", "record-send-total", Map.of());
        when(m.metricValue()).thenReturn(42L);
        registry.update(m);
        assertEquals(1, registry.snapshot().size());
    }

    @Test
    void integer_metric_value_is_included_in_snapshot() {
        MetricRegistry registry = new MetricRegistry();
        KafkaMetric m = metric("producer-metrics", "record-send-count", Map.of());
        when(m.metricValue()).thenReturn(7);
        registry.update(m);
        assertEquals(1, registry.snapshot().size());
    }

    @Test
    void non_numeric_metric_value_is_excluded_from_snapshot() {
        MetricRegistry registry = new MetricRegistry();
        KafkaMetric m = metric("producer-metrics", "record-send-rate", Map.of());
        when(m.metricValue()).thenReturn("not-a-number");
        registry.update(m);
        assertEquals(0, registry.snapshot().size());
    }

    @Test
    void predicate_filters_and_does_not_read_value_of_rejected_metrics() {
        // The predicate must be applied BEFORE metricValue() so a metric the export-time
        // allow-list discards never pays its synchronized value read. A metric whose
        // metricValue() throws proves the read is skipped: rejecting it must not blow up,
        // and accepting it must (the value is then read).
        MetricRegistry registry = new MetricRegistry();
        KafkaMetric kept = metric("producer-metrics", "record-send-rate", Map.of());
        KafkaMetric rejected = Mockito.mock(KafkaMetric.class);
        when(rejected.metricName()).thenReturn(new MetricName("obscure", "kafka.server", "", Map.of()));
        when(rejected.metricValue())
                .thenThrow(new AssertionError("metricValue() must not be read for a rejected metric"));
        registry.update(kept);
        registry.update(rejected);

        var snap = registry.snapshot(m -> m.metricName().group().equals("producer-metrics"));
        assertEquals(1, snap.size());
        assertEquals("record-send-rate", snap.get(0).metric().metricName().name());

        assertThrows(AssertionError.class, () -> registry.snapshot(m -> true));
    }
}
