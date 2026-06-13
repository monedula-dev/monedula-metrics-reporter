// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter;

import io.opentelemetry.api.common.Attributes;

/**
 * Immutable per-metric outputs that don't change once the metric is registered:
 * the OTel-flat metric name and the {@link Attributes} bundle built from the
 * metric's tags. Cached by the SPI mapper to avoid recomputing on every export
 * tick — the underlying inputs (Kafka {@code MetricName.tags()}, the configured
 * namespace, the formatter rules) are all fixed for the lifetime of a metric
 * registration.
 */
public final class MetricRendering {

    public final String name;
    public final Attributes attributes;
    /**
     * Whether this metric is a monotonic cumulative counter (Kafka SPI {@code *-total}
     * metrics) and should be emitted as an OTLP Sum rather than a Gauge, so PromQL
     * {@code rate()} is semantically correct. Fixed for the metric's lifetime, so it
     * lives in the cached rendering alongside the name and attributes.
     */
    public final boolean monotonic;

    public MetricRendering(String name, Attributes attributes, boolean monotonic) {
        this.name = name;
        this.attributes = attributes;
        this.monotonic = monotonic;
    }
}
