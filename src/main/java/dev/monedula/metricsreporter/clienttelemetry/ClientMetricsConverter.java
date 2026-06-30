// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter.clienttelemetry;

import com.google.protobuf.InvalidProtocolBufferException;
import dev.monedula.metricsreporter.clienttelemetry.ClientMetricsEnricher.ClientIdentity;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableDoublePointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableGaugeData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableHistogramData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableHistogramPointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableLongPointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSumData;
import io.opentelemetry.sdk.resources.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts a serialized OTLP {@code MetricsData} payload (pushed by a KIP-714 client) into
 * OTel SDK {@link MetricData} so it can be exported through the reporter's existing
 * {@code MetricExporter}. Each produced metric carries an enriched {@link Resource}.
 *
 * <p>Supported data-point families: gauge, sum, histogram (long or double). Any other type
 * is skipped and counted in {@link ConversionResult#unsupportedDropped()} — see the spec's
 * fidelity boundary.
 */
public final class ClientMetricsConverter {

    private static final Logger log = LoggerFactory.getLogger(ClientMetricsConverter.class);

    /** Result of a conversion: the produced metrics plus the count of dropped unsupported data points. */
    public record ConversionResult(List<MetricData> metrics, int unsupportedDropped) {}

    private final ClientMetricsEnricher enricher;

    public ClientMetricsConverter(ClientMetricsEnricher enricher) {
        this.enricher = enricher;
    }

    public ConversionResult convert(byte[] payload, Map<String, String> brokerIdentity, ClientIdentity clientIdentity)
            throws InvalidProtocolBufferException {
        MetricsData data = MetricsData.parseFrom(payload);
        List<MetricData> out = new ArrayList<>();
        int unsupported = 0;
        for (ResourceMetrics rm : data.getResourceMetricsList()) {
            Resource resource = enricher.enrich(rm.getResource(), brokerIdentity, clientIdentity);
            for (ScopeMetrics sm : rm.getScopeMetricsList()) {
                InstrumentationScopeInfo scope = scopeOf(sm);
                for (Metric m : sm.getMetricsList()) {
                    MetricData converted = convertMetric(resource, scope, m);
                    if (converted == null) {
                        unsupported++;
                    } else {
                        out.add(converted);
                    }
                }
            }
        }
        return new ConversionResult(out, unsupported);
    }

    private MetricData convertMetric(Resource resource, InstrumentationScopeInfo scope, Metric m) {
        MetricData converted =
                switch (m.getDataCase()) {
                    case GAUGE -> gauge(resource, scope, m);
                    case SUM -> sum(resource, scope, m);
                    case HISTOGRAM -> histogram(resource, scope, m);
                    default -> null;
                };
        if (converted == null) {
            log.debug("Dropping unsupported client metric '{}' (data case {})", m.getName(), m.getDataCase());
        }
        return converted;
    }

    private MetricData gauge(Resource resource, InstrumentationScopeInfo scope, Metric m) {
        List<NumberDataPoint> points = m.getGauge().getDataPointsList();
        if (points.isEmpty()) {
            return null;
        }
        // OTLP guarantees a consistent ValueCase across a metric's data points
        boolean isLong = points.get(0).getValueCase() == NumberDataPoint.ValueCase.AS_INT;
        if (isLong) {
            return ImmutableMetricData.createLongGauge(
                    resource,
                    scope,
                    m.getName(),
                    m.getDescription(),
                    m.getUnit(),
                    ImmutableGaugeData.create(buildLongPoints(points)));
        }
        return ImmutableMetricData.createDoubleGauge(
                resource,
                scope,
                m.getName(),
                m.getDescription(),
                m.getUnit(),
                ImmutableGaugeData.create(buildDoublePoints(points)));
    }

    private MetricData sum(Resource resource, InstrumentationScopeInfo scope, Metric m) {
        io.opentelemetry.proto.metrics.v1.Sum s = m.getSum();
        List<NumberDataPoint> points = s.getDataPointsList();
        if (points.isEmpty()) {
            return null;
        }
        boolean monotonic = s.getIsMonotonic();
        AggregationTemporality temporality = temporalityOf(s.getAggregationTemporality());
        // see gauge(): OTLP guarantees a consistent ValueCase across a metric's data points
        boolean isLong = points.get(0).getValueCase() == NumberDataPoint.ValueCase.AS_INT;
        if (isLong) {
            return ImmutableMetricData.createLongSum(
                    resource,
                    scope,
                    m.getName(),
                    m.getDescription(),
                    m.getUnit(),
                    ImmutableSumData.create(monotonic, temporality, buildLongPoints(points)));
        }
        return ImmutableMetricData.createDoubleSum(
                resource,
                scope,
                m.getName(),
                m.getDescription(),
                m.getUnit(),
                ImmutableSumData.create(monotonic, temporality, buildDoublePoints(points)));
    }

    private static List<io.opentelemetry.sdk.metrics.data.LongPointData> buildLongPoints(List<NumberDataPoint> points) {
        var pd = new ArrayList<io.opentelemetry.sdk.metrics.data.LongPointData>(points.size());
        for (NumberDataPoint p : points) {
            pd.add(ImmutableLongPointData.create(
                    p.getStartTimeUnixNano(), p.getTimeUnixNano(), attributesOf(p.getAttributesList()), p.getAsInt()));
        }
        return pd;
    }

    private static List<io.opentelemetry.sdk.metrics.data.DoublePointData> buildDoublePoints(
            List<NumberDataPoint> points) {
        var pd = new ArrayList<io.opentelemetry.sdk.metrics.data.DoublePointData>(points.size());
        for (NumberDataPoint p : points) {
            pd.add(ImmutableDoublePointData.create(
                    p.getStartTimeUnixNano(),
                    p.getTimeUnixNano(),
                    attributesOf(p.getAttributesList()),
                    p.getAsDouble()));
        }
        return pd;
    }

    private MetricData histogram(Resource resource, InstrumentationScopeInfo scope, Metric m) {
        io.opentelemetry.proto.metrics.v1.Histogram h = m.getHistogram();
        var points = h.getDataPointsList();
        if (points.isEmpty()) {
            return null;
        }
        AggregationTemporality temporality = temporalityOf(h.getAggregationTemporality());
        List<HistogramPointData> pd = new ArrayList<>(points.size());
        for (io.opentelemetry.proto.metrics.v1.HistogramDataPoint p : points) {
            pd.add(ImmutableHistogramPointData.create(
                    p.getStartTimeUnixNano(),
                    p.getTimeUnixNano(),
                    attributesOf(p.getAttributesList()),
                    p.getSum(),
                    p.hasMin(),
                    p.hasMin() ? p.getMin() : 0.0,
                    p.hasMax(),
                    p.hasMax() ? p.getMax() : 0.0,
                    p.getExplicitBoundsList(),
                    p.getBucketCountsList()));
        }
        return ImmutableMetricData.createDoubleHistogram(
                resource,
                scope,
                m.getName(),
                m.getDescription(),
                m.getUnit(),
                ImmutableHistogramData.create(temporality, pd));
    }

    private static AggregationTemporality temporalityOf(io.opentelemetry.proto.metrics.v1.AggregationTemporality t) {
        return t == io.opentelemetry.proto.metrics.v1.AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA
                ? AggregationTemporality.DELTA
                : AggregationTemporality.CUMULATIVE;
    }

    private static InstrumentationScopeInfo scopeOf(ScopeMetrics sm) {
        String name = sm.getScope().getName();
        String version = sm.getScope().getVersion();
        if (version.isEmpty()) {
            return InstrumentationScopeInfo.create(name.isEmpty() ? "kafka-client" : name);
        }
        return InstrumentationScopeInfo.builder(name.isEmpty() ? "kafka-client" : name)
                .setVersion(version)
                .build();
    }

    static Attributes attributesOf(List<KeyValue> kvs) {
        AttributesBuilder b = Attributes.builder();
        for (KeyValue kv : kvs) {
            b.put(
                    io.opentelemetry.api.common.AttributeKey.stringKey(kv.getKey()),
                    ClientMetricsEnricher.stringify(kv.getValue()));
        }
        return b.build();
    }
}
