// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter.yammer;

import static org.junit.jupiter.api.Assertions.*;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import java.util.AbstractMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class YammerMetricDataMapperTest {

    private final MetricsRegistry registry = new MetricsRegistry();
    private final YammerMetricDataMapper mapper = new YammerMetricDataMapper(Map.of());

    private Map.Entry<MetricName, Metric> entry(MetricName name, Metric m) {
        return new AbstractMap.SimpleImmutableEntry<>(name, m);
    }

    @Test
    void gauge_maps_to_double_gauge() {
        MetricName mn = new MetricName("kafka.server", "ReplicaManager", "UnderReplicatedPartitions");
        Gauge<Integer> g = new Gauge<Integer>() {
            public Integer value() {
                return 3;
            }
        };
        List<MetricData> result = mapper.mapAll(List.of(entry(mn, g)));
        assertEquals(1, result.size());
        assertEquals(MetricDataType.DOUBLE_GAUGE, result.get(0).getType());
        assertEquals(
                "kafka_server_replicamanager_underreplicatedpartitions",
                result.get(0).getName());
        double v =
                result.get(0).getDoubleGaugeData().getPoints().iterator().next().getValue();
        assertEquals(3.0, v, 0.001);
    }

    @Test
    void non_numeric_gauge_is_skipped() {
        MetricName mn = new MetricName("kafka.server", "BrokerInfo", "Version");
        Gauge<String> g = new Gauge<String>() {
            public String value() {
                return "4.2.0";
            }
        };
        assertTrue(mapper.mapAll(List.of(entry(mn, g))).isEmpty());
    }

    @Test
    void counter_maps_to_double_sum_monotonic_cumulative() {
        MetricName mn = new MetricName("kafka.server", "ReplicaManager", "IsrShrinksPerSec");
        Counter c = registry.newCounter(mn);
        c.inc(5);
        List<MetricData> result = mapper.mapAll(List.of(entry(mn, c)));
        assertEquals(MetricDataType.DOUBLE_SUM, result.get(0).getType());
        assertTrue(result.get(0).getDoubleSumData().isMonotonic());
        assertEquals(
                5.0,
                result.get(0).getDoubleSumData().getPoints().iterator().next().getValue(),
                0.001);
    }

    @Test
    void scope_parses_pairwise_into_attributes() {
        MetricName mn = new MetricName("kafka.server", "BrokerTopicMetrics", "BytesInPerSec", "topic.test-topic");
        // scope is "topic.test-topic"
        Counter c = registry.newCounter(mn);
        c.inc(10);
        List<MetricData> result = mapper.mapAll(List.of(entry(mn, c)));
        var attrs =
                result.get(0).getDoubleSumData().getPoints().iterator().next().getAttributes();
        assertEquals("test-topic", attrs.get(AttributeKey.stringKey("topic")));
    }

    @Test
    void odd_scope_falls_back_to_scope_attribute() {
        MetricName mn = new MetricName("kafka.server", "Foo", "Bar", "single");
        Counter c = registry.newCounter(mn);
        c.inc(1);
        var attrs = mapper.mapAll(List.of(entry(mn, c)))
                .get(0)
                .getDoubleSumData()
                .getPoints()
                .iterator()
                .next()
                .getAttributes();
        assertEquals("single", attrs.get(AttributeKey.stringKey("scope")));
    }

    @Test
    void per_partition_metric_produces_topic_and_partition_attributes() {
        // Reproduces how Kafka registers per-partition under-replicated metric:
        // group="kafka.cluster", type="Partition", name="UnderReplicated", scope="topic.test-topic.partition.0"
        MetricName mn = new MetricName("kafka.cluster", "Partition", "UnderReplicated", "topic.test-topic.partition.0");
        Gauge<Integer> g = new Gauge<Integer>() {
            public Integer value() {
                return 0;
            }
        };
        List<MetricData> result = mapper.mapAll(List.of(entry(mn, g)));
        assertEquals(1, result.size());
        assertEquals("kafka_cluster_partition_underreplicated", result.get(0).getName());
        var attrs =
                result.get(0).getDoubleGaugeData().getPoints().iterator().next().getAttributes();
        assertEquals("test-topic", attrs.get(AttributeKey.stringKey("topic")));
        assertEquals("0", attrs.get(AttributeKey.stringKey("partition")));
    }

    @Test
    void per_partition_metric_preserves_dotted_topic_name() {
        MetricName mn = new MetricName("kafka.cluster", "Partition", "UnderReplicated", "topic.orders.v1.partition.0");
        Gauge<Integer> g = new Gauge<Integer>() {
            public Integer value() {
                return 0;
            }
        };
        List<MetricData> result = mapper.mapAll(List.of(entry(mn, g)));
        assertEquals(1, result.size());
        var attrs =
                result.get(0).getDoubleGaugeData().getPoints().iterator().next().getAttributes();
        assertEquals("orders.v1", attrs.get(AttributeKey.stringKey("topic")));
        assertEquals("0", attrs.get(AttributeKey.stringKey("partition")));
    }

    @Test
    void per_partition_metric_with_multi_digit_partition() {
        MetricName mn =
                new MetricName("kafka.cluster", "Partition", "UnderReplicated", "topic.test-topic.partition.10");
        Gauge<Integer> g = new Gauge<Integer>() {
            public Integer value() {
                return 0;
            }
        };
        List<MetricData> result = mapper.mapAll(List.of(entry(mn, g)));
        var attrs =
                result.get(0).getDoubleGaugeData().getPoints().iterator().next().getAttributes();
        assertEquals("test-topic", attrs.get(AttributeKey.stringKey("topic")));
        assertEquals("10", attrs.get(AttributeKey.stringKey("partition")));
    }

    @Test
    void timer_emits_count_sum_plus_three_quantile_gauges() {
        // Timer is one of the broker's standard request-latency types (e.g.
        // RequestMetrics's TotalTimeMs). The mapper should produce one cumulative
        // Sum for the call count and three Gauges for p50 / p95 / p99 percentiles.
        MetricName mn = new MetricName("kafka.network", "RequestMetrics", "TotalTimeMs");
        Timer t = registry.newTimer(mn, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        t.update(10, TimeUnit.MILLISECONDS);
        t.update(20, TimeUnit.MILLISECONDS);
        t.update(50, TimeUnit.MILLISECONDS);

        List<MetricData> result = mapper.mapAll(List.of(entry(mn, t)));
        assertEquals(4, result.size(), "timer should produce 1 count Sum + 3 quantile Gauges");

        MetricData countMetric = result.stream()
                .filter(d -> d.getName().endsWith("_count"))
                .findFirst()
                .orElseThrow();
        assertEquals(MetricDataType.DOUBLE_SUM, countMetric.getType());
        assertEquals(
                3.0,
                countMetric.getDoubleSumData().getPoints().iterator().next().getValue(),
                0.001);

        Set<String> quantiles = result.stream()
                .filter(d -> d.getType() == MetricDataType.DOUBLE_GAUGE)
                .flatMap(d -> d.getDoubleGaugeData().getPoints().stream())
                .map(p -> p.getAttributes().get(AttributeKey.stringKey("quantile")))
                .collect(Collectors.toSet());
        assertEquals(Set.of("0.5", "0.95", "0.99"), quantiles);
    }

    @Test
    void histogram_emits_count_sum_plus_three_quantile_gauges() {
        // Histogram covers Kafka's request-size and message-size distributions
        // (e.g. RequestSizeInBytes). Same shape as Timer: a count Sum and three
        // quantile Gauges.
        MetricName mn = new MetricName("kafka.network", "RequestMetrics", "RequestSizeBytes");
        Histogram h = registry.newHistogram(mn, /*biased=*/ false);
        h.update(100);
        h.update(200);
        h.update(300);
        h.update(400);

        List<MetricData> result = mapper.mapAll(List.of(entry(mn, h)));
        assertEquals(4, result.size(), "histogram should produce 1 count Sum + 3 quantile Gauges");

        MetricData countMetric = result.stream()
                .filter(d -> d.getName().endsWith("_count"))
                .findFirst()
                .orElseThrow();
        assertEquals(MetricDataType.DOUBLE_SUM, countMetric.getType());
        assertEquals(
                4.0,
                countMetric.getDoubleSumData().getPoints().iterator().next().getValue(),
                0.001);

        Set<String> quantileNames = new HashSet<>();
        for (MetricData d : result) {
            if (d.getType() == MetricDataType.DOUBLE_GAUGE) {
                d.getDoubleGaugeData()
                        .getPoints()
                        .forEach(p -> quantileNames.add(p.getAttributes().get(AttributeKey.stringKey("quantile"))));
            }
        }
        assertEquals(Set.of("0.5", "0.95", "0.99"), quantileNames);
    }

    @Test
    void meter_emits_count_sum_only() {
        // Meter (Metered subtype) covers per-second rate metrics like
        // BrokerTopicMetrics.MessagesInPerSec. The mapper exports the cumulative
        // count() as a Sum and lets downstream tooling derive the per-second rate
        // (PromQL rate()), so it should NOT emit quantile gauges or anything else.
        MetricName mn = new MetricName("kafka.server", "BrokerTopicMetrics", "MessagesInPerSec");
        Meter m = registry.newMeter(mn, "messages", TimeUnit.SECONDS);
        m.mark();
        m.mark();
        m.mark(5);

        List<MetricData> result = mapper.mapAll(List.of(entry(mn, m)));
        assertEquals(1, result.size(), "meter should produce exactly one Sum, no quantile gauges");
        MetricData md = result.get(0);
        assertEquals(MetricDataType.DOUBLE_SUM, md.getType());
        assertEquals("kafka_server_brokertopicmetrics_messagesinpersec", md.getName());
        assertTrue(md.getDoubleSumData().isMonotonic());
        assertEquals(7.0, md.getDoubleSumData().getPoints().iterator().next().getValue(), 0.001);
    }

    @Test
    void caches_are_pruned_to_the_live_snapshot() {
        // Two topic-scoped metrics on distinct topics populate both caches; when one topic's
        // metric disappears from the snapshot, its name + scope entries must not linger
        // (otherwise scopeAttributesCache leaks one entry per topic-partition forever).
        MetricName a = new MetricName("kafka.server", "BrokerTopicMetrics", "BytesInPerSec", "topic.topic-a");
        MetricName b = new MetricName("kafka.server", "BrokerTopicMetrics", "BytesInPerSec", "topic.topic-b");
        Counter ca = registry.newCounter(a);
        Counter cb = registry.newCounter(b);

        mapper.mapAll(List.of(entry(a, ca), entry(b, cb)));
        assertEquals(2, mapper.nameCacheSize());
        assertEquals(2, mapper.scopeAttributesCacheSize());

        // topic-b's metric is gone this tick.
        mapper.mapAll(List.of(entry(a, ca)));
        assertEquals(1, mapper.nameCacheSize(), "stale name-cache entry was not pruned");
        assertEquals(1, mapper.scopeAttributesCacheSize(), "stale scope-cache entry was not pruned");
    }

    @Test
    void cumulative_sum_uses_supplied_start_epoch() {
        long startEpoch = 456_000_000_000L;
        var fixed = new YammerMetricDataMapper(Map.of(), startEpoch);
        MetricName mn = new MetricName("kafka.server", "ReplicaManager", "IsrShrinksPerSec");
        Counter c = registry.newCounter(mn);
        c.inc(3);
        MetricData data = fixed.mapAll(List.of(entry(mn, c))).get(0);
        assertEquals(
                startEpoch,
                data.getDoubleSumData().getPoints().iterator().next().getStartEpochNanos());
    }

    @Test
    void topic_scope_with_empty_partition_value_emits_topic_only() {
        MetricName mn = new MetricName("kafka.cluster", "Partition", "UnderReplicated", "topic.test-topic.partition.");
        Gauge<Integer> g = new Gauge<Integer>() {
            public Integer value() {
                return 0;
            }
        };
        var attrs = mapper.mapAll(List.of(entry(mn, g)))
                .get(0)
                .getDoubleGaugeData()
                .getPoints()
                .iterator()
                .next()
                .getAttributes();
        assertEquals("test-topic", attrs.get(AttributeKey.stringKey("topic")));
        assertNull(attrs.get(AttributeKey.stringKey("partition")));
    }
}
