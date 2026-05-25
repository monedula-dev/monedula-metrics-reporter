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
                    "ms",
                    lastExportDurationMs));

            var result = exporter.export(data);
            result.join(timeoutMs, TimeUnit.MILLISECONDS);
            success = result.isSuccess();
            if (!success) {
                log.warn("OTLP metric export failed or timed out — dropping this batch");
            }
        } catch (Exception e) {
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
