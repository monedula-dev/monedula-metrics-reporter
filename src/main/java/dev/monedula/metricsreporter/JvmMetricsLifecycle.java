// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter;

import io.opentelemetry.instrumentation.runtimetelemetry.RuntimeTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns the JVM-runtime-metrics side pipeline: a separate {@link SdkMeterProvider} that
 * feeds {@link RuntimeTelemetry} instruments (memory / GC / threads / classes / CPU) into
 * the same OTLP collector as the Kafka metrics flow. Independent of the main
 * {@link MetricCollector} so its lifecycle can be torn down and rebuilt when broker
 * context labels arrive after {@code configure()}.
 *
 * <p>Extracted from {@link OtlpMetricReporter} to give that class one fewer subsystem
 * to track and to make the JVM-side state changes easier to reason about in isolation.
 *
 * <p><b>Threading model.</b> Lifecycle methods ({@link #start},
 * {@link #rebuildIfRunning}, {@link #close}) are invoked from the same serialised
 * Kafka lifecycle thread that drives the reporter. The {@code sdk} field is the
 * "running" predicate — published last on {@link #start}, cleared first on
 * {@link #close} — so concurrent callers observe a consistent transition.
 */
final class JvmMetricsLifecycle {

    private static final Logger log = LoggerFactory.getLogger(JvmMetricsLifecycle.class);

    private final OtlpMetricReporterConfig cfg;

    private volatile OpenTelemetrySdk sdk;
    private volatile RuntimeTelemetry runtimeTelemetry;
    /**
     * Resource attached to the current JVM-metrics SdkMeterProvider. Compared against a
     * fresh resource in {@link #rebuildIfRunning} to short-circuit no-op rebuilds.
     * Package-private so tests can assert on it without reflecting into OTel internals.
     */
    volatile Resource resource;

    JvmMetricsLifecycle(OtlpMetricReporterConfig cfg) {
        this.cfg = cfg;
    }

    /**
     * Build the JVM-metrics SdkMeterProvider and start {@link RuntimeTelemetry}.
     *
     * <p>No JFR / JMX two-attempt dance here: the unified {@code runtime-telemetry}
     * module builds its JMX observables independently of its JFR stream and swallows a
     * failed JFR start internally (returning a JMX-only telemetry handle), so a host JVM
     * that cannot run JFR - a read-only {@code /tmp} in a slim container, say - still
     * gets the JMX-sourced memory / GC / thread / class / CPU instruments. The earlier
     * {@code runtime-telemetry-java17} module had no such behaviour and needed the
     * explicit {@code disableAllFeatures()} retry this method used to perform.
     */
    void start(Map<String, String> resourceAttrs) {
        Resource res = ResourceFactory.create(resourceAttrs);
        Handle handle = tryStart(res);
        if (handle == null) {
            return;
        }
        // Publish sdk last: it is the "ready" predicate used by rebuildIfRunning and
        // close(), so writes to resource/runtimeTelemetry happen-before the observable
        // transition from "not running" to "running".
        this.resource = res;
        this.runtimeTelemetry = handle.runtimeTelemetry;
        this.sdk = handle.sdk;
        log.info("OtlpMetricReporter JVM runtime metrics enabled");
    }

    /**
     * Tear down the current JVM-metrics pipeline (if running) and start a new one with
     * the given resource attributes. No-op when JVM metrics are disabled, never
     * started, or already running with an identical resource (avoids ripping down a
     * healthy JFR stream when {@code contextChange} is called twice with the same
     * labels).
     */
    void rebuildIfRunning(Map<String, String> resourceAttrs) {
        if (!cfg.jvmMetricsEnabled() || sdk == null) {
            return;
        }
        Resource newResource = ResourceFactory.create(resourceAttrs);
        if (newResource.equals(resource)) {
            return;
        }
        close();
        try {
            start(resourceAttrs);
        } catch (Throwable t) {
            log.warn("Failed to rebuild JVM runtime metrics after context change, continuing without them", t);
        }
    }

    /**
     * Shut down the JVM-metrics pipeline. Safe to call when {@link #start} never ran
     * or has already been closed.
     */
    void close() {
        // Clear sdk first: it is the entry guard used by rebuildIfRunning, so any
        // concurrent caller observes "not running" before the underlying objects start
        // closing. Mirror of the publish order in start().
        OpenTelemetrySdk s = this.sdk;
        this.sdk = null;
        RuntimeTelemetry rt = this.runtimeTelemetry;
        this.runtimeTelemetry = null;
        this.resource = null;

        // Close the SDK first so the PeriodicMetricReader's final flush picks up one
        // last reading from the runtime observables before they're shut down. Closing
        // RuntimeTelemetry first would make those observables inert and drop the last
        // interval.
        if (s != null) {
            closeSdk(s);
        }
        if (rt != null) {
            try {
                rt.close();
            } catch (Exception e) {
                log.warn("Error shutting down JVM RuntimeTelemetry", e);
            }
        }
    }

    private Handle tryStart(Resource resource) {
        OpenTelemetrySdk s = createSdk(resource);
        try {
            return new Handle(s, RuntimeTelemetry.create(s));
        } catch (Throwable t) {
            closeSdk(s);
            log.warn("JVM runtime metrics could not start ({}); continuing without them", t.toString());
            log.debug("JVM runtime metrics start failure detail", t);
            return null;
        }
    }

    private OpenTelemetrySdk createSdk(Resource resource) {
        MetricExporter jvmExporter = OtlpExporterFactory.create(cfg);
        SdkMeterProvider provider = SdkMeterProvider.builder()
                .setResource(resource)
                .registerMetricReader(PeriodicMetricReader.builder(jvmExporter)
                        .setInterval(Duration.ofMillis(cfg.intervalMs()))
                        .build())
                .build();
        return OpenTelemetrySdk.builder().setMeterProvider(provider).build();
    }

    private void closeSdk(OpenTelemetrySdk s) {
        // sdk.close() uses OTel's hardcoded 10s timeout; on broker shutdown a slow OTLP
        // collector would stall this path. Drive the shutdown explicitly with the
        // operator-configured timeout to match MetricCollector.stop().
        long timeoutMs = cfg.timeoutMs();
        try {
            var result = s.getSdkMeterProvider().shutdown().join(timeoutMs, TimeUnit.MILLISECONDS);
            if (!result.isSuccess()) {
                log.warn(
                        "JVM SDK shutdown did not complete cleanly within {}ms - "
                                + "background resources may still be active",
                        timeoutMs);
            }
        } catch (Exception e) {
            log.warn("Error shutting down JVM SDK", e);
        }
    }

    private record Handle(OpenTelemetrySdk sdk, RuntimeTelemetry runtimeTelemetry) {}
}
