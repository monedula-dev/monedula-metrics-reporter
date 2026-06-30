// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter;

import dev.monedula.metricsreporter.yammer.YammerMetricDataMapper;
import dev.monedula.metricsreporter.yammer.YammerMetricRegistry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableDoublePointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableGaugeData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSumData;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricCollector {

    private static final Logger log = LoggerFactory.getLogger(MetricCollector.class);

    private final MetricRegistry registry;
    private volatile MetricDataMapper mapper;
    private final MetricExporter exporter;
    private final long intervalMs;
    private final long timeoutMs;
    private final ScheduledExecutorService scheduler;

    // Optional Yammer side
    private volatile YammerMetricRegistry yammerRegistry;
    private volatile YammerMetricDataMapper yammerMapper;

    // Self-monitoring metrics: emitted in every export tick so operators have
    // continuous visibility into whether the reporter is actually shipping data.
    // The success/failure counters use LongAdder rather than AtomicLong because
    // the export thread is currently single-threaded but the read in exportTick
    // can race with future multi-export schemes; LongAdder costs nothing more.
    private final LongAdder exportSuccessCount = new LongAdder();
    private final LongAdder exportFailureCount = new LongAdder();
    /**
     * Captured at construction so the cumulative counters above carry a stable
     * {@code startEpochNanos} across every tick. Prometheus and other PromQL
     * tools rely on this to identify counter resets; if it jumped forward
     * every tick the counters would look like they reset constantly.
     */
    private final long startEpochNanos;
    /** Wall-clock duration of the previous tick in milliseconds. */
    private volatile long lastExportDurationMs;

    /**
     * Optional source of client-telemetry counters: {@code [received, forwarded, dropped,
     * unsupportedMetricsDropped, queueDepth]}. Null until a forwarder is wired (client telemetry
     * enabled). Read each tick so these self-metrics ride the same export as the rest of the
     * reporter self-monitoring.
     */
    private volatile Supplier<long[]> clientTelemetryCounters;

    public MetricCollector(
            MetricRegistry registry,
            MetricDataMapper mapper,
            MetricExporter exporter,
            long intervalMs,
            long timeoutMs) {
        this.registry = registry;
        this.mapper = mapper;
        this.exporter = exporter;
        this.intervalMs = intervalMs;
        this.timeoutMs = timeoutMs;
        this.startEpochNanos = System.currentTimeMillis() * 1_000_000L;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "monedula-metrics-reporter");
            t.setDaemon(true);
            t.setUncaughtExceptionHandler((th, ex) -> log.error("Uncaught exception in metric export thread", ex));
            return t;
        });
    }

    /** Attach a client-telemetry counter source: [received, forwarded, dropped, unsupportedMetricsDropped, queueDepth]. */
    public void setClientTelemetryCounters(Supplier<long[]> counters) {
        this.clientTelemetryCounters = counters;
    }

    /** Attach an optional Yammer source. May be called any time before/after start. */
    public void setYammer(YammerMetricRegistry yammerRegistry, YammerMetricDataMapper yammerMapper) {
        this.yammerRegistry = yammerRegistry;
        this.yammerMapper = yammerMapper;
    }

    /** Hot-swap mappers (used when context labels arrive after configure). */
    public void replaceMappers(MetricDataMapper newMapper, YammerMetricDataMapper newYammerMapper) {
        this.mapper = newMapper;
        this.yammerMapper = newYammerMapper;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::exportTick, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS)) {
                log.warn("Scheduler did not terminate within {}ms, forcing shutdownNow()", timeoutMs);
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        // If awaitTermination timed out, shutdownNow() has interrupted an in-flight tick that may
        // still be inside exporter.export() as we call exporter.shutdown() below. The OTLP exporter
        // tolerates this concurrent call; the worst case is a benign "did not complete cleanly"
        // warn-log. We accept that over adding more teardown latency on the Kafka close() thread —
        // bounded shutdown keeps broker shutdown responsive (Kafka availability first).
        var shutdownResult = exporter.shutdown().join(timeoutMs, TimeUnit.MILLISECONDS);
        if (!shutdownResult.isSuccess()) {
            log.warn(
                    "OTLP exporter shutdown did not complete cleanly within {}ms — "
                            + "background resources may still be active",
                    timeoutMs);
        }
    }

    /** Currently-installed SPI mapper. Visible for tests verifying contextChange swap. */
    MetricDataMapper currentMapper() {
        return mapper;
    }

    /** Run a single export synchronously. Visible for tests. */
    void exportOnceForTest() {
        exportTick();
    }

    /** Total exports that completed successfully since this collector started. Exposed for tests. */
    long exportSuccessCount() {
        return exportSuccessCount.sum();
    }

    /** Total exports that failed (timeout, connection refused, mapping exception). Exposed for tests. */
    long exportFailureCount() {
        return exportFailureCount.sum();
    }

    private void exportTick() {
        long tickStartNanos = System.nanoTime();
        boolean success = false;
        try {
            long now = System.currentTimeMillis() * 1_000_000L;
            List<MetricData> data = new ArrayList<>();
            data.addAll(mapper.mapAll(registry.snapshot()));
            YammerMetricRegistry yr = this.yammerRegistry;
            YammerMetricDataMapper ym = this.yammerMapper;
            if (yr != null && ym != null) {
                data.addAll(ym.mapAll(yr.snapshot()));
            }
            // Self-monitoring metrics. Emitted every tick so an operator can alert on
            // failure-rate or stalled export-duration even when the Kafka registry is
            // empty (e.g., just after broker startup). Reflects state up to the PREVIOUS
            // tick — this tick's success/failure increments after the export call below.
            data.add(selfSum(
                    now,
                    "monedula_reporter_export_success",
                    "Total successful OTLP metric exports since the reporter started",
                    exportSuccessCount.sum()));
            data.add(selfSum(
                    now,
                    "monedula_reporter_export_failure",
                    "Total failed OTLP metric exports (timeout, connection refused, mapping error) since the reporter started",
                    exportFailureCount.sum()));
            data.add(selfGauge(
                    now,
                    "monedula_reporter_export_duration_ms",
                    "Wall-clock duration of the previous export tick in milliseconds",
                    // Unit intentionally left blank: the "ms" is already in the metric name, and a
                    // non-empty OTLP unit makes the Prometheus exporter append a "_milliseconds"
                    // suffix (yielding monedula_reporter_export_duration_ms_milliseconds), which
                    // breaks the documented name and the quickstart kafka-metrics dashboard.
                    "",
                    lastExportDurationMs));

            Supplier<long[]> ctc = this.clientTelemetryCounters;
            if (ctc != null) {
                long[] c = ctc.get();
                data.add(selfSum(
                        now,
                        "monedula_reporter_clienttelemetry_received_total",
                        "Total KIP-714 client telemetry payloads accepted since the reporter started",
                        c[0]));
                data.add(selfSum(
                        now,
                        "monedula_reporter_clienttelemetry_forwarded_total",
                        "Total KIP-714 client telemetry payloads forwarded to the collector since the reporter started",
                        c[1]));
                data.add(selfSum(
                        now,
                        "monedula_reporter_clienttelemetry_dropped_total",
                        "Total KIP-714 client telemetry payloads dropped (queue full or export failure) since the reporter started",
                        c[2]));
                data.add(selfSum(
                        now,
                        "monedula_reporter_clienttelemetry_unsupported_metrics_dropped_total",
                        "Total client telemetry metric data points dropped as unsupported types since the reporter started",
                        c[3]));
                data.add(selfGauge(
                        now,
                        "monedula_reporter_clienttelemetry_queue_depth",
                        "Current depth of the client telemetry forwarder queue",
                        "",
                        c[4]));
            }

            var result = exporter.export(data);
            result.join(timeoutMs, TimeUnit.MILLISECONDS);
            success = result.isSuccess();
            if (!success) {
                log.warn("OTLP metric export failed or timed out — dropping this batch");
            }
        } catch (Throwable e) {
            // Catch Throwable, not just Exception: scheduleAtFixedRate silently cancels all
            // future executions if the task throws, and the captured throwable never reaches
            // the thread's uncaughtExceptionHandler — so an Error here (e.g. a first-tick
            // NoClassDefFoundError from lazily-loaded shaded classes) would stop export
            // permanently with no log. Fail open: log, count a failure, retry next tick.
            log.warn("Error during metric export tick — skipping batch", e);
            success = false;
        } finally {
            if (success) {
                exportSuccessCount.increment();
            } else {
                exportFailureCount.increment();
            }
            lastExportDurationMs = (System.nanoTime() - tickStartNanos) / 1_000_000L;
        }
    }

    private MetricData selfSum(long now, String name, String description, long count) {
        return ImmutableMetricData.createDoubleSum(
                mapper.resource(),
                ResourceFactory.SCOPE,
                name,
                description,
                "",
                ImmutableSumData.create(
                        true,
                        AggregationTemporality.CUMULATIVE,
                        List.of(ImmutableDoublePointData.create(
                                startEpochNanos, now, Attributes.empty(), (double) count))));
    }

    private MetricData selfGauge(long now, String name, String description, String unit, long value) {
        return ImmutableMetricData.createDoubleGauge(
                mapper.resource(),
                ResourceFactory.SCOPE,
                name,
                description,
                unit,
                ImmutableGaugeData.create(
                        List.of(ImmutableDoublePointData.create(now, now, Attributes.empty(), (double) value))));
    }
}
