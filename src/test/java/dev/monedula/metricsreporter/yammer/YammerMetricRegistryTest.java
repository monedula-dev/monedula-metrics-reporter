// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter.yammer;

import static org.junit.jupiter.api.Assertions.*;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class YammerMetricRegistryTest {

    private static List<Pattern> patterns(String... regexes) {
        List<Pattern> ps = new java.util.ArrayList<>();
        for (String r : regexes) ps.add(Pattern.compile(r));
        return ps;
    }

    @Test
    void captures_existing_metrics_on_attach() {
        MetricsRegistry registry = new MetricsRegistry();
        registry.newCounter(new MetricName("kafka.server", "ReplicaManager", "UnderReplicatedPartitions"));

        YammerMetricRegistry r = new YammerMetricRegistry(List.of());
        r.attach(registry);

        assertEquals(1, r.snapshot().size());
    }

    @Test
    void captures_metric_registered_during_initial_snapshot() {
        LateMetricRegistry registry = new LateMetricRegistry();

        YammerMetricRegistry r = new YammerMetricRegistry(List.of());
        r.attach(registry);

        assertTrue(r.snapshot().stream().anyMatch(e -> e.getKey().equals(registry.lateMetricName)));
    }

    @Test
    void captures_metrics_added_after_attach() {
        MetricsRegistry registry = new MetricsRegistry();
        YammerMetricRegistry r = new YammerMetricRegistry(List.of());
        r.attach(registry);
        registry.newCounter(new MetricName("kafka.controller", "KafkaController", "OfflinePartitionsCount"));
        assertEquals(1, r.snapshot().size());
    }

    @Test
    void removes_metric_on_remove_event() {
        MetricsRegistry registry = new MetricsRegistry();
        MetricName mn = new MetricName("kafka.server", "ReplicaManager", "UnderReplicatedPartitions");
        registry.newCounter(mn);
        YammerMetricRegistry r = new YammerMetricRegistry(List.of());
        r.attach(registry);
        registry.removeMetric(mn);
        assertEquals(0, r.snapshot().size());
    }

    @Test
    void allow_list_filters_at_registration() {
        MetricsRegistry registry = new MetricsRegistry();
        YammerMetricRegistry r = new YammerMetricRegistry(patterns("kafka\\.server\\..*"));
        r.attach(registry);
        registry.newCounter(new MetricName("kafka.server", "ReplicaManager", "UnderReplicatedPartitions"));
        registry.newCounter(new MetricName("kafka.network", "RequestMetrics", "RequestsPerSec"));
        assertEquals(1, r.snapshot().size());
    }

    @Test
    void detach_stops_receiving_events_and_clears_snapshot() {
        MetricsRegistry registry = new MetricsRegistry();
        YammerMetricRegistry r = new YammerMetricRegistry(List.of());
        r.attach(registry);
        registry.newCounter(new MetricName("kafka.server", "ReplicaManager", "UnderReplicatedPartitions"));
        assertEquals(1, r.snapshot().size());

        r.detach();
        assertEquals(0, r.snapshot().size());

        // Future events on the upstream registry must not flow back into us.
        registry.newCounter(new MetricName("kafka.controller", "KafkaController", "OfflinePartitionsCount"));
        assertEquals(0, r.snapshot().size());
    }

    @Test
    void detach_is_idempotent() {
        MetricsRegistry registry = new MetricsRegistry();
        YammerMetricRegistry r = new YammerMetricRegistry(List.of());
        r.attach(registry);
        r.detach();
        assertDoesNotThrow(r::detach);
    }

    private static final class LateMetricRegistry extends MetricsRegistry {
        private final MetricName lateMetricName =
                new MetricName("kafka.controller", "KafkaController", "ActiveControllerCount");
        private boolean registeredLateMetric;

        @Override
        public Map<MetricName, Metric> allMetrics() {
            Map<MetricName, Metric> snapshot = new LinkedHashMap<>(super.allMetrics());
            if (!registeredLateMetric) {
                registeredLateMetric = true;
                newCounter(lateMetricName);
            }
            return snapshot;
        }
    }
}
