// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;

/**
 * Holds the latest {@link KafkaMetric} object for every metric Kafka announces. Updated
 * directly from Kafka's hot add/change/remove callbacks, which do pure in-memory map work —
 * no filtering, no regex. Allow-list filtering happens later, on the export thread, in
 * {@link MetricCollector}; this registry deliberately stores everything so a widened
 * allow-list can resurrect metrics Kafka only ever announces once (e.g. via {@code init}).
 */
public class MetricRegistry {

    private final ConcurrentHashMap<MetricName, KafkaMetric> metrics = new ConcurrentHashMap<>();

    public void update(KafkaMetric metric) {
        metrics.put(metric.metricName(), metric);
    }

    public void remove(KafkaMetric metric) {
        metrics.remove(metric.metricName());
    }

    /**
     * A metric paired with the value observed for it at snapshot time. The value is read
     * exactly once, here, so the export thread never re-reads {@link KafkaMetric#metricValue()}
     * downstream — a second read could return a different (e.g. {@code NaN} for an empty
     * rate window) or non-numeric value and, if the mapper threw on it, drop the whole batch.
     * Reading once also halves the {@code synchronized} {@code metricValue()} calls per tick,
     * lowering contention with Kafka's metric-recording threads.
     */
    public record Sample(KafkaMetric metric, double value) {}

    public List<Sample> snapshot() {
        List<Sample> result = new ArrayList<>();
        for (KafkaMetric m : metrics.values()) {
            Object raw = m.metricValue();
            // Kafka Measurable impls return double, but Gauge<T> can return Long/Integer/
            // Float/etc. — accept any Number so those aren't silently dropped.
            if (!(raw instanceof Number)) continue;
            double v = ((Number) raw).doubleValue();
            if (Double.isFinite(v)) {
                result.add(new Sample(m, v));
            }
        }
        return result;
    }
}
