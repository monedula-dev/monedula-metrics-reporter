// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter.clienttelemetry;

import dev.monedula.metricsreporter.clienttelemetry.ClientMetricsConverter.ConversionResult;
import dev.monedula.metricsreporter.clienttelemetry.ClientMetricsEnricher.ClientIdentity;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drains inbound KIP-714 client telemetry off a bounded queue on a single daemon thread,
 * converts each payload to SDK {@link MetricData}, and exports it through its own
 * {@link MetricExporter} (built from the reporter config, isolated from the broker-metrics
 * collector). Honors the reporter's prime invariant: the request-handler thread only does
 * a non-blocking {@link #submit}; all parsing and I/O happen here. Failures drop the batch
 * and increment counters — nothing retries.
 *
 * <p>Counter granularity: {@code received}, {@code forwarded}, and {@code dropped} are
 * per-payload (one inbound telemetry push = one unit). {@code unsupportedMetricsDropped}
 * is per-metric: it counts individual metrics within a payload that the converter could
 * not represent, without marking the whole payload as dropped.
 */
public final class ClientTelemetryForwarder {

    private static final Logger log = LoggerFactory.getLogger(ClientTelemetryForwarder.class);

    /** One unit of inbound work: the copied payload bytes and the (optional) pushing client's identity. */
    private record Item(byte[] payload, ClientIdentity clientIdentity) {}

    private static final Item POISON = new Item(new byte[0], null);

    private final MetricExporter exporter;
    private final ClientMetricsConverter converter;
    private final BlockingQueue<Item> queue;
    private final long timeoutMs;

    private final LongAdder received = new LongAdder();
    private final LongAdder forwarded = new LongAdder();
    private final LongAdder dropped = new LongAdder();
    private final LongAdder unsupportedMetricsDropped = new LongAdder();

    private volatile Map<String, String> brokerIdentity = Map.of();
    private volatile Thread worker;
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public ClientTelemetryForwarder(
            MetricExporter exporter, ClientMetricsConverter converter, int queueCapacity, long timeoutMs) {
        this.exporter = exporter;
        this.converter = converter;
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.timeoutMs = timeoutMs;
    }

    /** Latest broker identity labels (cluster/node); updated on contextChange. */
    public void setBrokerIdentity(Map<String, String> brokerIdentity) {
        this.brokerIdentity = Map.copyOf(brokerIdentity);
    }

    /** Current broker-identity labels applied to forwarded client metrics. Visible for tests/observability. */
    public Map<String, String> currentBrokerIdentity() {
        return brokerIdentity;
    }

    public void start() {
        if (this.worker != null) {
            return;
        }
        Thread t = new Thread(this::run, "monedula-client-telemetry");
        t.setDaemon(true);
        t.setUncaughtExceptionHandler((th, ex) -> log.error("Uncaught exception in client telemetry thread", ex));
        this.worker = t;
        t.start();
    }

    /**
     * Enqueue a copied payload. Returns false (and counts a drop) when the queue is full —
     * never blocks the calling broker request-handler thread.
     */
    public boolean submit(byte[] payload, ClientIdentity clientIdentity) {
        received.increment();
        if (queue.offer(new Item(payload, clientIdentity))) {
            return true;
        }
        dropped.increment();
        return false;
    }

    private void run() {
        try {
            while (true) {
                Item item = queue.take();
                if (item == POISON) return;
                process(item);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void process(Item item) {
        try {
            ConversionResult result = converter.convert(item.payload(), brokerIdentity, item.clientIdentity());
            if (result.unsupportedDropped() > 0) {
                unsupportedMetricsDropped.add(result.unsupportedDropped());
            }
            List<MetricData> metrics = result.metrics();
            if (metrics.isEmpty()) {
                return;
            }
            var export = exporter.export(metrics);
            export.join(timeoutMs, TimeUnit.MILLISECONDS);
            if (export.isSuccess()) {
                forwarded.increment();
            } else {
                dropped.increment();
                log.warn("Client telemetry export failed or timed out — dropping batch");
            }
        } catch (Throwable t) {
            dropped.increment();
            log.warn("Error forwarding client telemetry — dropping batch", t);
        }
    }

    public void stop() {
        if (!stopped.compareAndSet(false, true)) {
            return; // already stopped — idempotent
        }
        Thread t = this.worker;
        if (t != null) {
            // The drain thread exits either by consuming POISON or, if the queue is full
            // so the offer fails, via the interrupt below (queue.take() throws InterruptedException).
            if (!queue.offer(POISON)) {
                log.debug("Client telemetry queue full at shutdown — relying on interrupt to stop the drain thread");
            }
            t.interrupt();
            try {
                t.join(timeoutMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // Always release the exporter, even if start() was never called (e.g. start() threw).
        exporter.shutdown().join(timeoutMs, TimeUnit.MILLISECONDS);
    }

    public long receivedCount() {
        return received.sum();
    }

    public long forwardedCount() {
        return forwarded.sum();
    }

    public long droppedCount() {
        return dropped.sum();
    }

    public long unsupportedMetricsDroppedCount() {
        return unsupportedMetricsDropped.sum();
    }

    public int queueDepth() {
        return queue.size();
    }
}
