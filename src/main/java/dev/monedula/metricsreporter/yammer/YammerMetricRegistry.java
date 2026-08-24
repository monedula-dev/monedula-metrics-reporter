// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter.yammer;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.MetricsRegistryListener;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Listens on a Yammer MetricsRegistry; stores every registered metric for periodic export.
 * Implements MetricsRegistryListener so we receive add/remove events as Kafka registers
 * its broker-internal metrics. Filtering (the allow-list) is applied later at export time
 * by {@code MetricCollector}, so the add/remove callbacks here do pure map put/remove.
 */
public class YammerMetricRegistry implements MetricsRegistryListener {

    private final ConcurrentHashMap<MetricName, Metric> metrics = new ConcurrentHashMap<>();
    private final Set<MetricsRegistry> attached =
            java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());

    /** Attach to a registry: capture currently-registered metrics + receive future events. */
    public void attach(MetricsRegistry registry) {
        for (Map.Entry<MetricName, Metric> e : registry.allMetrics().entrySet()) {
            metrics.put(e.getKey(), e.getValue());
        }
        registry.addListener(this);
        attached.add(registry);
    }

    /**
     * Reverse of {@link #attach}: remove this instance as a listener from every registry it
     * was attached to and drop the captured metric snapshot. Safe to call multiple times.
     * The global Yammer default registry outlives the reporter, so failing to detach would
     * keep this object reachable and cause duplicate events if a new reporter is configured.
     */
    public void detach() {
        for (MetricsRegistry registry : attached) {
            try {
                registry.removeListener(this);
            } catch (Exception ignored) {
                // Best-effort: a registry that no longer accepts removals shouldn't block teardown.
            }
        }
        attached.clear();
        metrics.clear();
    }

    @Override
    public void onMetricAdded(MetricName name, Metric metric) {
        metrics.put(name, metric);
    }

    @Override
    public void onMetricRemoved(MetricName name) {
        metrics.remove(name);
    }

    /** Snapshot of currently tracked (name, metric) pairs. */
    public Collection<Map.Entry<MetricName, Metric>> snapshot() {
        return new ArrayList<>(metrics.entrySet());
    }
}
