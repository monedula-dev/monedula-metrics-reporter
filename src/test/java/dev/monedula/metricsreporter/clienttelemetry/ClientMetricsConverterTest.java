// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter.clienttelemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ClientMetricsConverterTest {

    // stateless converter; safe to share across tests
    static final ClientMetricsConverter CONVERTER =
            new ClientMetricsConverter(new ClientMetricsEnricher(false, false, false, false));

    static MetricsData oneMetric(Metric metric) {
        return MetricsData.newBuilder()
                .addResourceMetrics(ResourceMetrics.newBuilder()
                        .addScopeMetrics(ScopeMetrics.newBuilder()
                                .setScope(InstrumentationScope.newBuilder()
                                        .setName("client")
                                        .build())
                                .addMetrics(metric)
                                .build())
                        .build())
                .build();
    }

    @Test
    void converts_double_gauge() throws Exception {
        Metric m = Metric.newBuilder()
                .setName("client.connection.count")
                .setGauge(Gauge.newBuilder()
                        .addDataPoints(NumberDataPoint.newBuilder()
                                .setTimeUnixNano(123L)
                                .setAsDouble(4.0)
                                .build())
                        .build())
                .build();

        var result = CONVERTER.convert(oneMetric(m).toByteArray(), Map.of(), null);
        assertEquals(0, result.unsupportedDropped());
        List<MetricData> data = result.metrics();
        assertEquals(1, data.size());
        assertEquals("client.connection.count", data.get(0).getName());
        assertEquals(MetricDataType.DOUBLE_GAUGE, data.get(0).getType());
        assertEquals(
                4.0,
                data.get(0).getDoubleGaugeData().getPoints().iterator().next().getValue());
    }

    @Test
    void converts_long_gauge() throws Exception {
        Metric m = Metric.newBuilder()
                .setName("client.assigned.partitions")
                .setGauge(Gauge.newBuilder()
                        .addDataPoints(NumberDataPoint.newBuilder()
                                .setTimeUnixNano(9L)
                                .setAsInt(7L)
                                .build())
                        .build())
                .build();

        var data = CONVERTER.convert(oneMetric(m).toByteArray(), Map.of(), null).metrics();
        assertEquals(MetricDataType.LONG_GAUGE, data.get(0).getType());
        assertEquals(
                7L, data.get(0).getLongGaugeData().getPoints().iterator().next().getValue());
    }

    @Test
    void converts_monotonic_cumulative_double_sum_preserving_timestamps() throws Exception {
        io.opentelemetry.proto.metrics.v1.Sum sum = io.opentelemetry.proto.metrics.v1.Sum.newBuilder()
                .setIsMonotonic(true)
                .setAggregationTemporality(
                        io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
                .addDataPoints(NumberDataPoint.newBuilder()
                        .setStartTimeUnixNano(100L)
                        .setTimeUnixNano(200L)
                        .setAsDouble(42.0)
                        .build())
                .build();
        Metric m = Metric.newBuilder().setName("client.bytes.sent").setSum(sum).build();

        var data = CONVERTER.convert(oneMetric(m).toByteArray(), Map.of(), null).metrics();
        assertEquals(MetricDataType.DOUBLE_SUM, data.get(0).getType());
        var sd = data.get(0).getDoubleSumData();
        assertTrue(sd.isMonotonic());
        assertEquals(AggregationTemporality.CUMULATIVE, sd.getAggregationTemporality());
        var point = sd.getPoints().iterator().next();
        assertEquals(100L, point.getStartEpochNanos());
        assertEquals(200L, point.getEpochNanos());
        assertEquals(42.0, point.getValue());
    }

    @Test
    void converts_delta_long_sum() throws Exception {
        io.opentelemetry.proto.metrics.v1.Sum sum = io.opentelemetry.proto.metrics.v1.Sum.newBuilder()
                .setIsMonotonic(false)
                .setAggregationTemporality(
                        io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA)
                .addDataPoints(NumberDataPoint.newBuilder()
                        .setTimeUnixNano(5L)
                        .setAsInt(3L)
                        .build())
                .build();
        Metric m = Metric.newBuilder().setName("client.errors").setSum(sum).build();

        var sd = CONVERTER
                .convert(oneMetric(m).toByteArray(), Map.of(), null)
                .metrics()
                .get(0)
                .getLongSumData();
        assertFalse(sd.isMonotonic());
        assertEquals(AggregationTemporality.DELTA, sd.getAggregationTemporality());
        assertEquals(3L, sd.getPoints().iterator().next().getValue());
    }

    @Test
    void converts_cumulative_histogram() throws Exception {
        io.opentelemetry.proto.metrics.v1.Histogram h = io.opentelemetry.proto.metrics.v1.Histogram.newBuilder()
                .setAggregationTemporality(
                        io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE)
                .addDataPoints(io.opentelemetry.proto.metrics.v1.HistogramDataPoint.newBuilder()
                        .setStartTimeUnixNano(1L)
                        .setTimeUnixNano(2L)
                        .setCount(3L)
                        .setSum(30.0)
                        .setMin(5.0)
                        .setMax(20.0)
                        .addExplicitBounds(10.0)
                        .addBucketCounts(1L)
                        .addBucketCounts(2L)
                        .build())
                .build();
        Metric m = Metric.newBuilder()
                .setName("client.request.latency")
                .setHistogram(h)
                .build();

        var data = CONVERTER.convert(oneMetric(m).toByteArray(), Map.of(), null).metrics();
        assertEquals(MetricDataType.HISTOGRAM, data.get(0).getType());
        var point = data.get(0).getHistogramData().getPoints().iterator().next();
        assertEquals(1L, point.getStartEpochNanos());
        assertEquals(2L, point.getEpochNanos());
        assertEquals(3L, point.getCount());
        assertEquals(30.0, point.getSum());
        assertEquals(5.0, point.getMin());
        assertEquals(20.0, point.getMax());
        assertEquals(List.of(10.0), point.getBoundaries());
        assertEquals(List.of(1L, 2L), point.getCounts());
    }

    @Test
    void drops_and_counts_exponential_histogram() throws Exception {
        Metric m = Metric.newBuilder()
                .setName("client.exp")
                .setExponentialHistogram(io.opentelemetry.proto.metrics.v1.ExponentialHistogram.newBuilder()
                        .addDataPoints(io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint.newBuilder()
                                .setTimeUnixNano(1L)
                                .setCount(1L)
                                .build())
                        .build())
                .build();

        var result = CONVERTER.convert(oneMetric(m).toByteArray(), Map.of(), null);
        assertEquals(0, result.metrics().size());
        assertEquals(1, result.unsupportedDropped());
    }
}
