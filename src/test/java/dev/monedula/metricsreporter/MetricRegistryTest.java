// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
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

    private static List<Pattern> patterns(String... regexes) {
        return List.of(regexes).stream().map(Pattern::compile).toList();
    }

    @Test
    void metric_is_stored_when_no_allow_list() {
        MetricRegistry registry = new MetricRegistry(List.of());
        registry.update(metric("producer-metrics", "record-send-rate", Map.of()));
        assertEquals(1, registry.snapshot().size());
    }

    @Test
    void metric_is_stored_when_regex_matches_group_dot_name() {
        MetricRegistry registry = new MetricRegistry(patterns("producer-metrics\\..*"));
        registry.update(metric("producer-metrics", "record-send-rate", Map.of()));
        assertEquals(1, registry.snapshot().size());
    }

    @Test
    void metric_is_rejected_when_no_pattern_matches() {
        MetricRegistry registry = new MetricRegistry(patterns("consumer.*"));
        registry.update(metric("producer-metrics", "record-send-rate", Map.of()));
        assertEquals(0, registry.snapshot().size());
    }

    @Test
    void any_matching_pattern_allows_the_metric() {
        MetricRegistry registry = new MetricRegistry(patterns("consumer.*", "producer.*"));
        registry.update(metric("producer-metrics", "record-send-rate", Map.of()));
        assertEquals(1, registry.snapshot().size());
    }

    @Test
    void metric_is_removed() {
        MetricRegistry registry = new MetricRegistry(List.of());
        KafkaMetric m = metric("producer-metrics", "record-send-rate", Map.of());
        registry.update(m);
        registry.remove(m);
        assertEquals(0, registry.snapshot().size());
    }

    @Test
    void snapshot_returns_independent_copy() {
        MetricRegistry registry = new MetricRegistry(List.of());
        KafkaMetric m = metric("producer-metrics", "record-send-rate", Map.of());
        registry.update(m);
        var snap = registry.snapshot();
        registry.remove(m);
        assertEquals(1, snap.size()); // snapshot not affected by subsequent remove
    }

    @Test
    void nan_metric_value_is_excluded_from_snapshot() {
        MetricRegistry registry = new MetricRegistry(List.of());
        KafkaMetric m = metric("producer-metrics", "record-send-rate", Map.of());
        when(m.metricValue()).thenReturn(Double.NaN);
        registry.update(m);
        assertEquals(0, registry.snapshot().size());
    }

    @Test
    void infinite_metric_value_is_excluded_from_snapshot() {
        MetricRegistry registry = new MetricRegistry(List.of());
        KafkaMetric m = metric("producer-metrics", "record-send-rate", Map.of());
        when(m.metricValue()).thenReturn(Double.POSITIVE_INFINITY);
        registry.update(m);
        assertEquals(0, registry.snapshot().size());
    }

    @Test
    void long_metric_value_is_included_in_snapshot() {
        MetricRegistry registry = new MetricRegistry(List.of());
        KafkaMetric m = metric("producer-metrics", "record-send-total", Map.of());
        when(m.metricValue()).thenReturn(42L);
        registry.update(m);
        assertEquals(1, registry.snapshot().size());
    }

    @Test
    void integer_metric_value_is_included_in_snapshot() {
        MetricRegistry registry = new MetricRegistry(List.of());
        KafkaMetric m = metric("producer-metrics", "record-send-count", Map.of());
        when(m.metricValue()).thenReturn(7);
        registry.update(m);
        assertEquals(1, registry.snapshot().size());
    }

    @Test
    void non_numeric_metric_value_is_excluded_from_snapshot() {
        MetricRegistry registry = new MetricRegistry(List.of());
        KafkaMetric m = metric("producer-metrics", "record-send-rate", Map.of());
        when(m.metricValue()).thenReturn("not-a-number");
        registry.update(m);
        assertEquals(0, registry.snapshot().size());
    }
}
