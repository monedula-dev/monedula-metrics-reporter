// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter;

import com.yammer.metrics.core.MetricsRegistry;
import dev.monedula.metricsreporter.yammer.YammerMetricDataMapper;
import dev.monedula.metricsreporter.yammer.YammerMetricRegistry;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unified OTLP metric reporter for Kafka. Bridges both:
 *  - the Kafka SPI metrics registry (clients + broker request/network metrics), and
 *  - the Kafka Yammer/Coda Hale metrics registry (broker-internal metrics like
 *    UnderReplicatedPartitions, ActiveControllerCount, BrokerTopicMetrics, ...).
 *
 * <p>The Yammer side is wired only when running inside a broker — we probe for
 * {@code org.apache.kafka.server.metrics.KafkaYammerMetrics} via reflection so the
 * same JAR works unchanged on clients (where that class is absent from the classpath).
 *
 * <p>Broker context (cluster.id, broker.id, kafka.version) arrives via
 * {@link #contextChange(MetricsContext)} and is attached to every exported metric as
 * an OTLP Resource attribute (rendered as Prometheus labels).
 *
 * <p><b>Threading model.</b> Kafka invokes the lifecycle methods
 * ({@link #configure}, {@link #contextChange}, {@link #close}) serially from a single
 * thread during broker/client startup and shutdown. The hot metric-callback path
 * ({@link #metricChange}, {@link #metricRemoval}, {@link #init}) can run concurrently
 * with itself but is guarded by {@code volatile} reads of {@code registry} and the
 * {@code noop} flag. JVM-metrics setup and teardown are delegated to
 * {@link JvmMetricsLifecycle}, which owns its own volatile "running" predicate.
 */
public class OtlpMetricReporter implements MetricsReporter {

    private static final Logger log = LoggerFactory.getLogger(OtlpMetricReporter.class);

    private volatile MetricRegistry registry;
    private volatile MetricCollector collector;
    /**
     * Yammer side of the reporter. Stays {@code null} on client-only classpaths (no
     * {@code KafkaYammerMetrics}) — kept package-private so tests can assert on
     * client-mode behavior without reflecting into the reporter's internals.
     */
    volatile YammerMetricRegistry yammerRegistry;

    private volatile OtlpMetricReporterConfig cfg;
    private volatile Map<String, String> contextLabels = Collections.emptyMap();
    private volatile String namespace = "";
    private volatile boolean noop = false;
    /**
     * Owner of the JVM-runtime-metrics side pipeline. {@code null} when JVM metrics
     * are disabled or before {@link #configure} has run. Package-private so tests can
     * read its {@code resource} field without going through OTel internals.
     */
    volatile JvmMetricsLifecycle jvmMetrics;

    /**
     * Per-reporter {@code service.instance.id} (OTel semantic convention). Generated
     * once when this object is constructed and never changes, so cumulative counters
     * carry a stable identity across {@link #configure} reloads on the same instance.
     * A fresh reporter object (typically a JVM restart) gets a fresh ID. Operators
     * who want a deterministic value can override via the
     * {@code otlp.metric.reporter.resource.attributes} config — that key wins over
     * this auto-generated default.
     */
    private final String instanceId = UUID.randomUUID().toString();

    @Override
    public void contextChange(MetricsContext metricsContext) {
        try {
            Map<String, String> labels = metricsContext == null ? null : metricsContext.contextLabels();
            if (labels == null) {
                this.contextLabels = Collections.emptyMap();
                this.namespace = "";
            } else {
                this.namespace = labels.getOrDefault(MetricsContext.NAMESPACE, "");
                Map<String, String> filtered = new LinkedHashMap<>(labels);
                filtered.remove(MetricsContext.NAMESPACE);
                this.contextLabels = filtered;
            }
            log.info("OtlpMetricReporter contextChange namespace={} labels={}", this.namespace, this.contextLabels);
            // If we've already configured, rebuild the mapper so the new labels take effect.
            rebuildMappersIfRunning();
        } catch (Exception e) {
            log.warn("OtlpMetricReporter contextChange failed — continuing without context labels", e);
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // Allow a re-init attempt after a previous failure: clear the noop flag up front so
        // a successful run leaves the reporter live, and tear down any leftovers from a prior
        // partial init in case configure() is being called a second time.
        noop = false;
        teardown();
        try {
            cfg = new OtlpMetricReporterConfig(configs);
            registry = new MetricRegistry(cfg.allowedMetrics());

            Map<String, String> resourceAttrs = combinedResourceAttributes();
            MetricDataMapper mapper = new MetricDataMapper(namespace, resourceAttrs);
            registry.setEvictionListener(mapper::onMetricRemoved);

            collector = new MetricCollector(
                    registry, mapper, OtlpExporterFactory.create(cfg), cfg.intervalMs(), cfg.timeoutMs());

            attachYammerIfBroker(cfg, resourceAttrs);

            collector.start();
            log.info(
                    "OtlpMetricReporter started — endpoint={} transport={} interval={}ms yammer={}",
                    cfg.endpoint(),
                    cfg.transport(),
                    cfg.intervalMs(),
                    yammerRegistry != null);

            if (cfg.jvmMetricsEnabled()) {
                try {
                    jvmMetrics = new JvmMetricsLifecycle(cfg);
                    jvmMetrics.start(resourceAttrs);
                } catch (Throwable t) {
                    log.warn("Failed to start JVM runtime metrics, continuing without them", t);
                }
            }
        } catch (Exception e) {
            // Kafka availability comes first (see docs/assumptions.md): a bad reporter config
            // must not crash the broker. Configure() exceptions propagate through
            // AbstractConfig.getConfiguredInstances and fail broker startup, so we swallow
            // here, log at ERROR for operator visibility, and run as no-op.
            log.error("OtlpMetricReporter failed to initialize, running as no-op", e);
            teardown();
            noop = true;
        }
    }

    /**
     * Returns the OTLP {@link io.opentelemetry.sdk.resources.Resource} currently
     * attached to every Kafka-SPI metric this reporter exports. Reflects the latest
     * {@link #contextChange} merge. Returns {@code null} when {@link #configure} has
     * not yet completed or the reporter is in no-op mode. Visible for tests.
     */
    io.opentelemetry.sdk.resources.Resource currentResource() {
        MetricCollector c = this.collector;
        if (c == null) return null;
        MetricDataMapper m = c.currentMapper();
        return m == null ? null : m.resource();
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        if (noop || registry == null) return;
        metrics.forEach(registry::update);
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        if (noop || registry == null) return;
        registry.update(metric);
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        if (noop || registry == null) return;
        registry.remove(metric);
    }

    @Override
    public void close() {
        teardown();
    }

    /**
     * Release any resources that may have been allocated during {@link #configure}: the
     * scheduled export thread and its OTLP exporter, plus the optional JVM-runtime SDK.
     * Safe to call on a partially-initialized or already-closed reporter — each step is
     * null-guarded and idempotent, and fields are reset so a subsequent {@code configure()}
     * starts clean. JVM-metrics pipeline is torn down AFTER the main collector so the main
     * pipeline gets to drain first.
     */
    private void teardown() {
        MetricCollector c = this.collector;
        if (c != null) {
            try {
                c.stop();
            } catch (Exception e) {
                log.warn("Error shutting down metric collector", e);
            }
            this.collector = null;
        }
        YammerMetricRegistry yr = this.yammerRegistry;
        if (yr != null) {
            try {
                yr.detach();
            } catch (Exception e) {
                log.warn("Error detaching Yammer registry listener", e);
            }
        }
        JvmMetricsLifecycle jm = this.jvmMetrics;
        if (jm != null) {
            try {
                jm.close();
            } catch (Exception e) {
                log.warn("Error shutting down JVM metrics", e);
            }
            this.jvmMetrics = null;
        }
        this.registry = null;
        this.yammerRegistry = null;
    }

    // --- helpers -----------------------------------------------------------

    /**
     * Merge order (later wins):
     *   1. Kafka broker context labels (cluster id, broker/node id, kafka version).
     *   2. Auto-generated {@code service.instance.id} - so two reporters in the same
     *      broker JVM are distinguishable in the collector even if they share broker
     *      context.
     *   3. User-supplied {@code resource.attributes} from config - operators can
     *      override the auto ID if they need a deterministic value.
     */
    private Map<String, String> combinedResourceAttributes() {
        Map<String, String> merged = new LinkedHashMap<>();
        merged.putAll(contextLabels);
        merged.put("service.instance.id", instanceId);
        if (cfg != null) merged.putAll(cfg.resourceAttributes());
        return merged;
    }

    private void rebuildMappersIfRunning() {
        MetricCollector c = collector;
        if (c == null) return;
        Map<String, String> resourceAttrs = combinedResourceAttributes();
        MetricDataMapper newMapper = new MetricDataMapper(namespace, resourceAttrs);
        YammerMetricDataMapper newYammerMapper =
                yammerRegistry != null ? new YammerMetricDataMapper(resourceAttrs) : null;
        c.replaceMappers(newMapper, newYammerMapper);
        // Re-point the registries' eviction listeners at the new mappers' caches —
        // the old mapper instances become unreachable so their caches GC out naturally.
        if (registry != null) {
            registry.setEvictionListener(newMapper::onMetricRemoved);
        }
        if (yammerRegistry != null && newYammerMapper != null) {
            yammerRegistry.setEvictionListener(newYammerMapper::onMetricRemoved);
        }
        JvmMetricsLifecycle jm = jvmMetrics;
        if (jm != null) {
            jm.rebuildIfRunning(resourceAttrs);
        }
    }

    /**
     * Reflection-based broker detection. {@code KafkaYammerMetrics} is only present
     * on the broker classpath. On clients we silently skip the Yammer wiring.
     */
    private void attachYammerIfBroker(OtlpMetricReporterConfig cfg, Map<String, String> resourceAttrs) {
        MetricsRegistry yammer;
        try {
            Class<?> klass = Class.forName("org.apache.kafka.server.metrics.KafkaYammerMetrics");
            yammer = (MetricsRegistry) klass.getMethod("defaultRegistry").invoke(null);
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            log.debug(
                    "KafkaYammerMetrics not available — Yammer metric collection disabled (likely client-side process)");
            return;
        } catch (Exception e) {
            log.warn("Failed to access KafkaYammerMetrics registry, skipping Yammer collection", e);
            return;
        }

        YammerMetricRegistry yr = new YammerMetricRegistry(cfg.allowedMetrics());
        yr.attach(yammer);
        // Also attach to the default Yammer registry — some Kafka internals still register there.
        try {
            MetricsRegistry defaultRegistry = com.yammer.metrics.Metrics.defaultRegistry();
            if (defaultRegistry != yammer) {
                yr.attach(defaultRegistry);
            }
        } catch (NoClassDefFoundError e) {
            // metrics-core not on the classpath — shouldn't happen if KafkaYammerMetrics loaded
        }

        YammerMetricDataMapper yammerMapper = new YammerMetricDataMapper(resourceAttrs);
        yr.setEvictionListener(yammerMapper::onMetricRemoved);
        this.yammerRegistry = yr;
        collector.setYammer(yr, yammerMapper);
        log.info("OtlpMetricReporter attached to Kafka Yammer registry — broker-internal metrics enabled");
    }
}
