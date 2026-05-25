// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter;

import static io.opentelemetry.api.common.AttributeKey.stringKey;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.resources.Resource;
import java.util.Map;

/**
 * Single source of truth for the OTLP Resource and InstrumentationScope attached
 * to every metric stream this reporter emits — Kafka SPI metrics
 * ({@link MetricDataMapper}), Yammer metrics
 * ({@link dev.monedula.metricsreporter.yammer.YammerMetricDataMapper}), and JVM runtime
 * metrics (set up in {@link OtlpMetricReporter}).
 *
 * <p>Keeping the construction in one place guarantees that all three streams
 * carry identical {@code kafka_cluster_id} / {@code kafka_node_id} resource
 * labels, so they're joinable in Prometheus by broker identity.
 */
public final class ResourceFactory {

    public static final String SERVICE_NAME = "kafka-otlp-metric-reporter";

    public static final InstrumentationScopeInfo SCOPE = InstrumentationScopeInfo.create(SERVICE_NAME);

    private ResourceFactory() {}

    /** Build a Resource from a merged map of resource attributes. */
    public static Resource create(Map<String, String> resourceAttributes) {
        Resource base =
                Resource.getDefault().merge(Resource.create(attributesOf(Map.of("service.name", SERVICE_NAME))));
        return resourceAttributes.isEmpty() ? base : base.merge(Resource.create(attributesOf(resourceAttributes)));
    }

    /** Pack a tag map into an OTel {@link Attributes} bundle. */
    static Attributes attributesOf(Map<String, String> tags) {
        AttributesBuilder b = Attributes.builder();
        tags.forEach((k, v) -> b.put(stringKey(k), v));
        return b.build();
    }
}
