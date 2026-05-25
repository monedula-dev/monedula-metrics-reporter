// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;

public class MetricRegistry {

    private final AllowList allowList;
    private final ConcurrentHashMap<MetricName, KafkaMetric> metrics = new ConcurrentHashMap<>();
    private volatile Consumer<MetricName> evictionListener;

    public MetricRegistry(List<Pattern> allowedPatterns) {
        this.allowList = new AllowList(allowedPatterns);
    }

    /**
     * Register a listener notified when a metric leaves the registry. Used by
     * {@link MetricDataMapper} to invalidate its per-metric rendering cache so
     * removed metrics don't pile up indefinitely. Only one listener is kept;
     * setting a new one replaces the previous binding.
     */
    public void setEvictionListener(Consumer<MetricName> listener) {
        this.evictionListener = listener;
    }

    public void update(KafkaMetric metric) {
        if (isAllowed(metric)) {
            metrics.put(metric.metricName(), metric);
        }
    }

    public void remove(KafkaMetric metric) {
        MetricName name = metric.metricName();
        metrics.remove(name);
        Consumer<MetricName> l = this.evictionListener;
        if (l != null) l.accept(name);
    }

    public List<KafkaMetric> snapshot() {
        List<KafkaMetric> result = new ArrayList<>();
        for (KafkaMetric m : metrics.values()) {
            Object raw = m.metricValue();
            // Kafka Measurable impls return double, but Gauge<T> can return Long/Integer/
            // Float/etc. — accept any Number so those aren't silently dropped.
            if (!(raw instanceof Number)) continue;
            double v = ((Number) raw).doubleValue();
            if (Double.isFinite(v)) {
                result.add(m);
            }
        }
        return result;
    }

    private boolean isAllowed(KafkaMetric metric) {
        return allowList.matches(
                metric.metricName().group() + "." + metric.metricName().name());
    }
}
