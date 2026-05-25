// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter;

import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableDoublePointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableGaugeData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.resources.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;

public class MetricDataMapper {

    private final Resource resource;
    private final String namespace;
    /**
     * Per-{@link MetricName} cached {@link MetricRendering} — formatted name + Attributes
     * bundle. Inputs (tags, namespace, formatter rules) are immutable for a metric's
     * lifetime, so a tick that re-encounters a previously-seen metric just reads from
     * here instead of rebuilding the {@link io.opentelemetry.api.common.Attributes} +
     * sanitising the name. {@link MetricRegistry} drives eviction via its listener.
     */
    private final ConcurrentHashMap<MetricName, MetricRendering> renderingCache = new ConcurrentHashMap<>();

    public MetricDataMapper(Map<String, String> resourceAttributes) {
        this("", resourceAttributes);
    }

    public MetricDataMapper(String namespace, Map<String, String> resourceAttributes) {
        this.namespace = namespace == null ? "" : namespace;
        this.resource = ResourceFactory.create(resourceAttributes);
    }

    /**
     * The OTLP {@link Resource} attached to every metric this mapper produces.
     * Exposed so the collector can attach the same resource to self-monitoring
     * metrics it emits alongside the Kafka stream.
     */
    public Resource resource() {
        return resource;
    }

    public MetricData map(KafkaMetric metric) {
        Object raw = metric.metricValue();
        if (!(raw instanceof Number)) {
            throw new IllegalArgumentException("Non-numeric metric value for " + metric.metricName() + ": " + raw);
        }
        double value = ((Number) raw).doubleValue();
        if (!Double.isFinite(value)) {
            throw new IllegalArgumentException("Non-finite metric value for " + metric.metricName() + ": " + value);
        }
        long now = System.currentTimeMillis() * 1_000_000L;
        MetricRendering r = renderingCache.computeIfAbsent(metric.metricName(), this::computeRendering);

        return ImmutableMetricData.createDoubleGauge(
                resource,
                ResourceFactory.SCOPE,
                r.name,
                metric.metricName().description(),
                "",
                ImmutableGaugeData.create(List.of(ImmutableDoublePointData.create(now, now, r.attributes, value))));
    }

    public List<MetricData> mapAll(List<KafkaMetric> metrics) {
        List<MetricData> result = new ArrayList<>(metrics.size());
        for (KafkaMetric m : metrics) {
            result.add(map(m));
        }
        return result;
    }

    /** Drop the cached rendering for a removed metric. Wired up by {@link MetricRegistry}. */
    public void onMetricRemoved(MetricName name) {
        renderingCache.remove(name);
    }

    private MetricRendering computeRendering(MetricName n) {
        String name = namespace.isEmpty()
                ? MetricNameFormatter.format(n.group(), n.name())
                : MetricNameFormatter.sanitize(namespace + "_" + n.group() + "_" + n.name());
        return new MetricRendering(name, ResourceFactory.attributesOf(n.tags()));
    }
}
