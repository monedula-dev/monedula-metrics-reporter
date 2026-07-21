// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter.clienttelemetry;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ClientTelemetryForwarderTest {

    /** Captures exported metrics; can be told to fail to exercise the export-failure counter. */
    static final class CapturingExporter implements MetricExporter {
        final List<MetricData> exported = java.util.Collections.synchronizedList(new ArrayList<>());
        volatile boolean fail = false;

        @Override
        public CompletableResultCode export(Collection<MetricData> metrics) {
            if (fail) return CompletableResultCode.ofFailure();
            exported.addAll(metrics);
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode flush() {
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode shutdown() {
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public AggregationTemporality getAggregationTemporality(InstrumentType instrumentType) {
            return AggregationTemporality.CUMULATIVE;
        }
    }

    static byte[] oneGauge() {
        return MetricsData.newBuilder()
                .addResourceMetrics(ResourceMetrics.newBuilder()
                        .addScopeMetrics(ScopeMetrics.newBuilder()
                                .setScope(InstrumentationScope.newBuilder()
                                        .setName("client")
                                        .build())
                                .addMetrics(Metric.newBuilder()
                                        .setName("client.count")
                                        .setGauge(Gauge.newBuilder()
                                                .addDataPoints(NumberDataPoint.newBuilder()
                                                        .setTimeUnixNano(1L)
                                                        .setAsDouble(1.0)
                                                        .build())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build()
                .toByteArray();
    }

    static ClientTelemetryForwarder newForwarder(MetricExporter exporter, int capacity) {
        return new ClientTelemetryForwarder(
                exporter,
                new ClientMetricsConverter(new ClientMetricsEnricher(false, false, false, false)),
                capacity,
                1000L);
    }

    private static void awaitEquals(long expected, java.util.function.LongSupplier actual) throws InterruptedException {
        for (int i = 0; i < 200 && actual.getAsLong() != expected; i++) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        assertEquals(expected, actual.getAsLong());
    }

    @Test
    void forwards_converted_metrics_to_exporter() throws Exception {
        var exporter = new CapturingExporter();
        var fwd = newForwarder(exporter, 16);
        fwd.start();
        try {
            fwd.submit(oneGauge(), null);
            awaitEquals(1, fwd::forwardedCount);
            assertEquals("client.count", exporter.exported.get(0).getName());
            assertEquals(1, fwd.receivedCount());
            assertEquals(0, fwd.droppedCount());
        } finally {
            fwd.stop();
        }
    }

    @Test
    void replace_converter_takes_effect_for_the_next_payload() throws Exception {
        var exporter = new CapturingExporter();
        // newForwarder's enricher has enrichBroker=false, so broker identity is not attached.
        var fwd = newForwarder(exporter, 16);
        fwd.start();
        try {
            fwd.submit(oneGauge(), null);
            awaitEquals(1, fwd::forwardedCount);
            assertNull(
                    exporter.exported.get(0).getResource().getAttributes().get(stringKey("kafka.cluster.id")),
                    "with enrichBroker=false the exported Resource must not carry broker identity");

            // Swap in a converter that enriches with broker identity, and supply that identity.
            fwd.setBrokerIdentity(Map.of("kafka.cluster.id", "C1"));
            fwd.replaceConverter(new ClientMetricsConverter(new ClientMetricsEnricher(true, false, false, false)));

            fwd.submit(oneGauge(), null);
            awaitEquals(2, fwd::forwardedCount);
            assertEquals(
                    "C1",
                    exporter.exported.get(1).getResource().getAttributes().get(stringKey("kafka.cluster.id")),
                    "after replaceConverter with enrichBroker=true the exported Resource must carry broker identity");
        } finally {
            fwd.stop();
        }
    }

    @Test
    void counts_export_failure() throws Exception {
        var exporter = new CapturingExporter();
        exporter.fail = true;
        var fwd = newForwarder(exporter, 16);
        fwd.start();
        try {
            fwd.submit(oneGauge(), null);
            awaitEquals(1, fwd::droppedCount);
            assertEquals(0, fwd.forwardedCount());
        } finally {
            fwd.stop();
        }
    }

    @Test
    void drops_when_queue_full() {
        // Capacity 1, never started, so nothing drains: first submit fills the queue, rest are dropped.
        var fwd = newForwarder(new CapturingExporter(), 1);
        AtomicInteger accepted = new AtomicInteger();
        for (int i = 0; i < 5; i++) {
            if (fwd.submit(oneGauge(), null)) accepted.incrementAndGet();
        }
        assertEquals(1, accepted.get());
        assertTrue(fwd.droppedCount() >= 4);
    }

    @Test
    void submit_after_stop_is_rejected_and_counted_as_dropped() throws Exception {
        var fwd = newForwarder(new CapturingExporter(), 16);
        fwd.start();
        fwd.stop();
        // The drain thread is gone: an unguarded offer would park the payload invisibly on a
        // dead queue — neither forwarded nor dropped — until the queue filled. It must instead
        // be rejected and counted as dropped immediately.
        assertFalse(fwd.submit(oneGauge(), null));
        assertEquals(1, fwd.receivedCount());
        assertEquals(1, fwd.droppedCount());
        assertEquals(0, fwd.forwardedCount());
    }

    @Test
    void counts_unsupported_metrics_separately_from_payload_drops() throws Exception {
        var exporter = new CapturingExporter();
        var fwd = newForwarder(exporter, 16);
        fwd.start();
        try {
            byte[] payload = MetricsData.newBuilder()
                    .addResourceMetrics(ResourceMetrics.newBuilder()
                            .addScopeMetrics(ScopeMetrics.newBuilder()
                                    .setScope(InstrumentationScope.newBuilder()
                                            .setName("client")
                                            .build())
                                    .addMetrics(Metric.newBuilder()
                                            .setName("client.exp")
                                            .setExponentialHistogram(
                                                    io.opentelemetry.proto.metrics.v1.ExponentialHistogram.newBuilder()
                                                            .addDataPoints(io.opentelemetry.proto.metrics.v1
                                                                    .ExponentialHistogramDataPoint.newBuilder()
                                                                    .setTimeUnixNano(1L)
                                                                    .setCount(1L)
                                                                    .build())
                                                            .build())
                                            .build())
                                    .build())
                            .build())
                    .build()
                    .toByteArray();
            fwd.submit(payload, null);
            awaitEquals(1, fwd::unsupportedMetricsDroppedCount);
            assertEquals(0, fwd.forwardedCount());
            assertEquals(0, fwd.droppedCount());
        } finally {
            fwd.stop();
        }
    }
}
