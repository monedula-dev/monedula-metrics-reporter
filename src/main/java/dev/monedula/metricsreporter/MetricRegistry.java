// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;

public class MetricRegistry {

    private final AllowList allowList;
    private final ConcurrentHashMap<MetricName, KafkaMetric> metrics = new ConcurrentHashMap<>();

    public MetricRegistry(List<Pattern> allowedPatterns) {
        this.allowList = new AllowList(allowedPatterns);
    }

    public void update(KafkaMetric metric) {
        if (isAllowed(metric)) {
            metrics.put(metric.metricName(), metric);
        }
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

    private boolean isAllowed(KafkaMetric metric) {
        return allowList.matches(
                metric.metricName().group() + "." + metric.metricName().name());
    }
}
