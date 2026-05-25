// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter.yammer;

import static io.opentelemetry.api.common.AttributeKey.stringKey;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.stats.Snapshot;
import dev.monedula.metricsreporter.MetricNameFormatter;
import dev.monedula.metricsreporter.ResourceFactory;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableDoublePointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableGaugeData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSumData;
import io.opentelemetry.sdk.resources.Resource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class YammerMetricDataMapper {

    private static final double[] QUANTILES = {0.5, 0.95, 0.99};

    private final Resource resource;
    /**
     * Constant start-of-process time used as {@code startEpochNanos} for all CUMULATIVE
     * sum points. Capturing this once (rather than reusing {@code now} each tick) keeps
     * the start time stable across exports, so downstream tools (Prometheus / PromQL
     * {@code rate()}) don't misread it as a counter reset on every interval.
     */
    private final long startEpochNanos;
    /**
     * Yammer {@code MetricName.getScope()} is immutable for the lifetime of the metric,
     * so the parsed {@link Attributes} is too. Cache by scope string to skip per-tick
     * substring/lastIndexOf work on brokers with thousands of topic-partition metrics.
     * Bounded by the number of distinct Yammer scopes (≈ topics + a handful).
     */
    private final ConcurrentHashMap<String, Attributes> scopeAttributesCache = new ConcurrentHashMap<>();
    /**
     * Per-{@link MetricName} cached, sanitised metric name. Yammer MetricName is immutable,
     * so {@code group + type + name} -&gt; flat-lowercase string can be computed once and reused
     * on every export tick. {@link YammerMetricRegistry} drives eviction via its listener.
     */
    private final ConcurrentHashMap<MetricName, String> nameCache = new ConcurrentHashMap<>();

    public YammerMetricDataMapper(Map<String, String> resourceAttributes) {
        this.resource = ResourceFactory.create(resourceAttributes);
        this.startEpochNanos = System.currentTimeMillis() * 1_000_000L;
    }

    public List<MetricData> mapAll(Collection<Map.Entry<MetricName, Metric>> snapshot) {
        long now = System.currentTimeMillis() * 1_000_000L;
        List<MetricData> result = new ArrayList<>(snapshot.size());
        for (Map.Entry<MetricName, Metric> entry : snapshot) {
            MetricName n = entry.getKey();
            Metric m = entry.getValue();
            String name = nameCache.computeIfAbsent(n, YammerMetricDataMapper::formatName);
            Attributes baseAttrs = cachedAttributesForScope(n.getScope());

            if (m instanceof Gauge) {
                MetricData md = gaugeMetric(now, name, baseAttrs, (Gauge<?>) m);
                if (md != null) result.add(md);
            } else if (m instanceof Counter) {
                result.add(sumMetric(now, name, baseAttrs, ((Counter) m).count()));
            } else if (m instanceof Timer) {
                Timer t = (Timer) m;
                result.add(sumMetric(now, name + "_count", baseAttrs, t.count()));
                addQuantileGauges(result, now, name, baseAttrs, t.getSnapshot());
            } else if (m instanceof Histogram) {
                Histogram h = (Histogram) m;
                result.add(sumMetric(now, name + "_count", baseAttrs, h.count()));
                addQuantileGauges(result, now, name, baseAttrs, h.getSnapshot());
            } else if (m instanceof Metered) {
                result.add(sumMetric(now, name, baseAttrs, ((Metered) m).count()));
            }
            // else: unknown metric type, skip
        }
        return result;
    }

    private MetricData gaugeMetric(long now, String name, Attributes attrs, Gauge<?> g) {
        Object v = g.value();
        if (!(v instanceof Number)) return null;
        double d = ((Number) v).doubleValue();
        if (!Double.isFinite(d)) return null;
        return ImmutableMetricData.createDoubleGauge(
                resource,
                ResourceFactory.SCOPE,
                name,
                "",
                "",
                ImmutableGaugeData.create(List.of(ImmutableDoublePointData.create(now, now, attrs, d))));
    }

    private MetricData sumMetric(long now, String name, Attributes attrs, long count) {
        return ImmutableMetricData.createDoubleSum(
                resource,
                ResourceFactory.SCOPE,
                name,
                "",
                "",
                ImmutableSumData.create(
                        true,
                        AggregationTemporality.CUMULATIVE,
                        List.of(ImmutableDoublePointData.create(startEpochNanos, now, attrs, (double) count))));
    }

    private void addQuantileGauges(
            List<MetricData> out, long now, String baseName, Attributes baseAttrs, Snapshot snap) {
        for (double q : QUANTILES) {
            double v = snap.getValue(q);
            if (!Double.isFinite(v)) continue;
            Attributes withQuantile = baseAttrs.toBuilder()
                    .put(stringKey("quantile"), String.valueOf(q))
                    .build();
            out.add(ImmutableMetricData.createDoubleGauge(
                    resource,
                    ResourceFactory.SCOPE,
                    baseName,
                    "",
                    "",
                    ImmutableGaugeData.create(List.of(ImmutableDoublePointData.create(now, now, withQuantile, v)))));
        }
    }

    /** Drop the cached name for a removed metric. Wired up by {@link YammerMetricRegistry}. */
    public void onMetricRemoved(MetricName name) {
        nameCache.remove(name);
        // scopeAttributesCache is keyed by scope string and shared across metrics with the
        // same scope (e.g. all per-partition metrics on a topic share one entry), so it
        // would be wrong to evict here on a single-metric removal.
    }

    private static String formatName(MetricName n) {
        return MetricNameFormatter.sanitize(n.getGroup() + "_" + n.getType() + "_" + n.getName());
    }

    private Attributes cachedAttributesForScope(String scope) {
        if (scope == null || scope.isEmpty()) return Attributes.empty();
        return scopeAttributesCache.computeIfAbsent(scope, YammerMetricDataMapper::attributesFromScope);
    }

    static Attributes attributesFromScope(String scope) {
        if (scope == null || scope.isEmpty()) return Attributes.empty();
        if (scope.startsWith("topic.")) return attributesFromKafkaTopicScope(scope);
        return attributesFromPairwiseScope(scope);
    }

    private static Attributes attributesFromPairwiseScope(String scope) {
        String[] parts = scope.split("\\.");
        if (parts.length % 2 != 0) {
            return Attributes.builder().put(stringKey("scope"), scope).build();
        }
        AttributesBuilder b = Attributes.builder();
        for (int i = 0; i < parts.length; i += 2) {
            b.put(stringKey(parts[i]), parts[i + 1]);
        }
        return b.build();
    }

    private static Attributes attributesFromKafkaTopicScope(String scope) {
        String value = scope.substring("topic.".length());
        if (value.isEmpty()) return Attributes.empty();

        String partitionMarker = ".partition.";
        int partitionIndex = value.lastIndexOf(partitionMarker);
        if (partitionIndex < 0) {
            return Attributes.builder().put(stringKey("topic"), value).build();
        }
        String topic = value.substring(0, partitionIndex);
        String partition = value.substring(partitionIndex + partitionMarker.length());
        if (topic.isEmpty()) return Attributes.empty();
        AttributesBuilder b = Attributes.builder().put(stringKey("topic"), topic);
        // partition is best-effort: emit only when it looks like a Kafka partition id (ASCII digits).
        if (!partition.isEmpty() && isAsciiDigits(partition)) {
            b.put(stringKey("partition"), partition);
        }
        return b.build();
    }

    private static boolean isAsciiDigits(String s) {
        for (int i = 0, n = s.length(); i < n; i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') return false;
        }
        return true;
    }
}
