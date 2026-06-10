// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter.yammer;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.MetricsRegistryListener;
import dev.monedula.metricsreporter.AllowList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Listens on a Yammer MetricsRegistry; stores allow-listed metrics for periodic export.
 * Implements MetricsRegistryListener so we receive add/remove events as Kafka registers
 * its broker-internal metrics.
 */
public class YammerMetricRegistry implements MetricsRegistryListener {

    private final AllowList allowList;
    private final ConcurrentHashMap<MetricName, Metric> metrics = new ConcurrentHashMap<>();
    private final Set<MetricsRegistry> attached =
            java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());

    public YammerMetricRegistry(List<Pattern> allowedPatterns) {
        this.allowList = new AllowList(allowedPatterns);
    }

    /** Attach to a registry: capture currently-registered metrics + receive future events. */
    public void attach(MetricsRegistry registry) {
        for (Map.Entry<MetricName, Metric> e : registry.allMetrics().entrySet()) {
            if (isAllowed(e.getKey())) metrics.put(e.getKey(), e.getValue());
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
        if (isAllowed(name)) metrics.put(name, metric);
    }

    @Override
    public void onMetricRemoved(MetricName name) {
        metrics.remove(name);
    }

    /** Snapshot of currently tracked (name, metric) pairs. */
    public Collection<Map.Entry<MetricName, Metric>> snapshot() {
        return new ArrayList<>(metrics.entrySet());
    }

    private boolean isAllowed(MetricName metric) {
        return allowList.matches(metric.getGroup() + "." + metric.getType() + "." + metric.getName());
    }
}
