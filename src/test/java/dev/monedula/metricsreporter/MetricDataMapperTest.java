// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class MetricDataMapperTest {

    private KafkaMetric metric(String group, String name, Map<String, String> tags, double value) {
        KafkaMetric m = Mockito.mock(KafkaMetric.class);
        MetricName mn = new MetricName(name, group, "desc", tags);
        when(m.metricName()).thenReturn(mn);
        when(m.metricValue()).thenReturn(value);
        return m;
    }

    @Test
    void maps_to_double_gauge() {
        var mapper = new MetricDataMapper(Map.of());
        KafkaMetric m = metric("producer-metrics", "record-send-rate", Map.of("client-id", "p1"), 42.0);
        MetricData data = mapper.map(m);
        assertEquals(MetricDataType.DOUBLE_GAUGE, data.getType());
    }

    @Test
    void metric_name_uses_formatter() {
        var mapper = new MetricDataMapper(Map.of());
        KafkaMetric m = metric("producer-metrics", "record-send-rate", Map.of(), 1.0);
        MetricData data = mapper.map(m);
        assertEquals("producer_metrics_record_send_rate", data.getName());
    }

    @Test
    void tags_become_attributes() {
        var mapper = new MetricDataMapper(Map.of());
        KafkaMetric m = metric("producer-metrics", "record-send-rate", Map.of("client-id", "p1"), 1.0);
        MetricData data = mapper.map(m);
        var points = data.getDoubleGaugeData().getPoints();
        assertEquals(1, points.size());
        assertEquals(
                "p1",
                points.iterator()
                        .next()
                        .getAttributes()
                        .get(io.opentelemetry.api.common.AttributeKey.stringKey("client-id")));
    }

    @Test
    void gauge_value_is_preserved() {
        var mapper = new MetricDataMapper(Map.of());
        KafkaMetric m = metric("producer-metrics", "record-send-rate", Map.of(), 99.5);
        MetricData data = mapper.map(m);
        double actual = data.getDoubleGaugeData().getPoints().iterator().next().getValue();
        assertEquals(99.5, actual, 0.001);
    }

    @Test
    void throws_for_non_numeric_metric_value() {
        var mapper = new MetricDataMapper(Map.of());
        KafkaMetric m = Mockito.mock(KafkaMetric.class);
        MetricName mn = new MetricName("rate", "producer-metrics", "", Map.of());
        when(m.metricName()).thenReturn(mn);
        when(m.metricValue()).thenReturn("not-a-double");
        assertThrows(IllegalArgumentException.class, () -> mapper.map(m));
    }

    @Test
    void long_metric_value_is_converted_to_double() {
        var mapper = new MetricDataMapper(Map.of());
        KafkaMetric m = Mockito.mock(KafkaMetric.class);
        MetricName mn = new MetricName("offset", "consumer-metrics", "", Map.of());
        when(m.metricName()).thenReturn(mn);
        when(m.metricValue()).thenReturn(123_456_789L);
        MetricData data = mapper.map(m);
        double actual = data.getDoubleGaugeData().getPoints().iterator().next().getValue();
        assertEquals(123_456_789.0, actual, 0.0);
    }

    @Test
    void integer_metric_value_is_converted_to_double() {
        var mapper = new MetricDataMapper(Map.of());
        KafkaMetric m = Mockito.mock(KafkaMetric.class);
        MetricName mn = new MetricName("count", "consumer-metrics", "", Map.of());
        when(m.metricName()).thenReturn(mn);
        when(m.metricValue()).thenReturn(7);
        MetricData data = mapper.map(m);
        double actual = data.getDoubleGaugeData().getPoints().iterator().next().getValue();
        assertEquals(7.0, actual, 0.0);
    }

    @Test
    void maps_list_of_metrics() {
        var mapper = new MetricDataMapper(Map.of());
        var metrics = List.of(
                metric("producer-metrics", "record-send-rate", Map.of(), 1.0),
                metric("producer-metrics", "record-error-rate", Map.of(), 2.0));
        assertEquals(2, mapper.mapAll(metrics).size());
    }

    @Test
    void namespace_prefix_is_prepended_when_set() {
        var mapper = new MetricDataMapper("kafka.server", Map.of());
        KafkaMetric m = metric("producer-metrics", "record-send-rate", Map.of(), 1.0);
        MetricData data = mapper.map(m);
        assertEquals("kafka_server_producer_metrics_record_send_rate", data.getName());
    }

    @Test
    void empty_namespace_falls_back_to_unprefixed_name() {
        var mapper = new MetricDataMapper("", Map.of());
        KafkaMetric m = metric("producer-metrics", "record-send-rate", Map.of(), 1.0);
        MetricData data = mapper.map(m);
        assertEquals("producer_metrics_record_send_rate", data.getName());
    }
}
