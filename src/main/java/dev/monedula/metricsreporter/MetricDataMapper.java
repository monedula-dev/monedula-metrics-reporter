// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter;

import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableDoublePointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableGaugeData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSumData;
import io.opentelemetry.sdk.resources.Resource;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;

public class MetricDataMapper {

    private final Resource resource;
    private final String namespace;
    /**
     * Constant start-of-process time used as {@code startEpochNanos} for every CUMULATIVE
     * Sum point (the {@code *-total} counters). Captured once so the start time stays
     * stable across export ticks — and threaded through {@code contextChange} mapper
     * rebuilds (see {@link OtlpMetricReporter}) so a label change doesn't look like a
     * counter reset to PromQL {@code rate()}.
     */
    private final long startEpochNanos;
    /**
     * Per-{@link MetricName} cached {@link MetricRendering} — formatted name + Attributes
     * bundle + counter/gauge classification. Inputs (tags, namespace, formatter rules) are
     * immutable for a metric's lifetime, so a tick that re-encounters a previously-seen
     * metric just reads from here. Entries for removed metrics are pruned in {@link #mapAll}
     * by retaining only the live snapshot's names — so invalidation happens on the export
     * thread, never on Kafka's hot metric-removal callback path.
     */
    private final ConcurrentHashMap<MetricName, MetricRendering> renderingCache = new ConcurrentHashMap<>();

    public MetricDataMapper(Map<String, String> resourceAttributes) {
        this("", resourceAttributes, System.currentTimeMillis() * 1_000_000L);
    }

    public MetricDataMapper(String namespace, Map<String, String> resourceAttributes) {
        this(namespace, resourceAttributes, System.currentTimeMillis() * 1_000_000L);
    }

    public MetricDataMapper(String namespace, Map<String, String> resourceAttributes, long startEpochNanos) {
        this.namespace = namespace == null ? "" : namespace;
        this.resource = ResourceFactory.create(resourceAttributes);
        this.startEpochNanos = startEpochNanos;
    }

    /**
     * The OTLP {@link Resource} attached to every metric this mapper produces.
     * Exposed so the collector can attach the same resource to self-monitoring
     * metrics it emits alongside the Kafka stream.
     */
    public Resource resource() {
        return resource;
    }

    /** Stable cumulative-counter start time; threaded into rebuilt mappers to avoid reset signals. */
    public long startEpochNanos() {
        return startEpochNanos;
    }

    /**
     * Map a single live metric. Reads {@link KafkaMetric#metricValue()} and throws on a
     * non-numeric / non-finite value — used only for direct callers (tests). The export
     * path goes through {@link #mapAll(List)} with values already captured at snapshot time.
     */
    public MetricData map(KafkaMetric metric) {
        Object raw = metric.metricValue();
        if (!(raw instanceof Number)) {
            throw new IllegalArgumentException("Non-numeric metric value for " + metric.metricName() + ": " + raw);
        }
        double value = ((Number) raw).doubleValue();
        if (!Double.isFinite(value)) {
            throw new IllegalArgumentException("Non-finite metric value for " + metric.metricName() + ": " + value);
        }
        return map(System.currentTimeMillis() * 1_000_000L, metric, value);
    }

    public List<MetricData> mapAll(List<MetricRegistry.Sample> samples) {
        long now = System.currentTimeMillis() * 1_000_000L;
        List<MetricData> result = new ArrayList<>(samples.size());
        for (MetricRegistry.Sample s : samples) {
            result.add(map(now, s.metric(), s.value()));
        }
        pruneCacheTo(samples);
        return result;
    }

    private MetricData map(long now, KafkaMetric metric, double value) {
        MetricRendering r = renderingCache.computeIfAbsent(metric.metricName(), this::computeRendering);
        if (r.monotonic) {
            return ImmutableMetricData.createDoubleSum(
                    resource,
                    ResourceFactory.SCOPE,
                    r.name,
                    metric.metricName().description(),
                    "",
                    ImmutableSumData.create(
                            true,
                            AggregationTemporality.CUMULATIVE,
                            List.of(ImmutableDoublePointData.create(startEpochNanos, now, r.attributes, value))));
        }
        return ImmutableMetricData.createDoubleGauge(
                resource,
                ResourceFactory.SCOPE,
                r.name,
                metric.metricName().description(),
                "",
                ImmutableGaugeData.create(List.of(ImmutableDoublePointData.create(now, now, r.attributes, value))));
    }

    /**
     * Evict cached renderings for metrics no longer present in the live snapshot. Runs on
     * the export thread after mapping, so a metric removal racing with an in-flight tick
     * can't leave a permanently-stale entry (the next tick's snapshot won't contain it).
     */
    private void pruneCacheTo(List<MetricRegistry.Sample> samples) {
        if (renderingCache.size() <= samples.size()) return;
        Set<MetricName> live = new HashSet<>(samples.size() * 2);
        for (MetricRegistry.Sample s : samples) {
            live.add(s.metric().metricName());
        }
        renderingCache.keySet().retainAll(live);
    }

    /** Current rendering-cache entry count. Visible for tests verifying live-set pruning. */
    int renderingCacheSize() {
        return renderingCache.size();
    }

    private MetricRendering computeRendering(MetricName n) {
        String name = namespace.isEmpty()
                ? MetricNameFormatter.format(n.group(), n.name())
                : MetricNameFormatter.sanitize(namespace + "_" + n.group() + "_" + n.name());
        // Kafka SPI cumulative counters are named "<something>-total" (e.g. record-send-total).
        // Emit those as monotonic Sums so PromQL rate() is correct; everything else is a Gauge.
        boolean monotonic = n.name().endsWith("-total");
        return new MetricRendering(name, ResourceFactory.attributesOf(n.tags()), monotonic);
    }
}
