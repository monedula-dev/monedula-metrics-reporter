// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter.bench;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.monedula.metricsreporter.OtlpMetricReporter;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.ToxiproxyContainer.ContainerProxy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * Drives OtlpMetricReporter against a Toxiproxy-fronted OTel collector with a
 * 5-second latency toxic injected, while the reporter's timeout is 500 ms.
 *
 * <p>Backs the "slow collector can't stall Kafka - export times out, batch
 * drops, next tick is fresh" claim in architecture.md and README.md. This is
 * the TCP-accepted-but-stalled case, distinct from the TCP-refused case
 * already covered by OtlpExportIntegrationTest.collector_unavailable_does_not_throw.
 */
@Testcontainers
class ExportTimeoutBehaviorTest {

    @Test
    void slow_collector_does_not_stall_export_scheduler() throws Exception {
        try (Network network = Network.newNetwork()) {
            GenericContainer<?> collector = null;
            ToxiproxyContainer toxiproxy = null;
            try {
                collector = new GenericContainer<>("otel/opentelemetry-collector-contrib:0.152.0")
                        .withNetwork(network)
                        .withNetworkAliases("otel-collector")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("otel-collector-bench-config.yaml"),
                                "/etc/otel-collector-config.yaml")
                        .withCommand("--config=/etc/otel-collector-config.yaml")
                        .withExposedPorts(4317)
                        .waitingFor(Wait.forLogMessage(".*Everything is ready.*", 1));
                collector.start();

                // ToxiproxyContainer's String constructor verifies the image
                // matches its default repo (`shopify/toxiproxy`). Use the
                // DockerImageName form so we can pin a specific GHCR tag
                // without that check rejecting it.
                toxiproxy = new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.9.0")
                                .asCompatibleSubstituteFor("shopify/toxiproxy"))
                        .withNetwork(network);
                toxiproxy.start();

                // ToxiproxyContainer pre-exposes ports 8666-8697 for proxy
                // listeners. getProxy() picks one, registers an inbound proxy
                // to the named target, and returns a ContainerProxy handle
                // whose getOriginalProxyPort() is the inside-container port
                // that Testcontainers has already mapped to a host port.
                ContainerProxy proxy = toxiproxy.getProxy(collector, 4317);
                proxy.toxics().latency("slow-collector", ToxicDirection.DOWNSTREAM, 5000L);

                String reporterEndpoint =
                        "http://" + toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(proxy.getOriginalProxyPort());

                Map<String, String> config = new HashMap<>();
                config.put("otlp.metric.reporter.endpoint", reporterEndpoint);
                config.put("otlp.metric.reporter.transport", "grpc");
                config.put("otlp.metric.reporter.interval.ms", "1000");
                config.put("otlp.metric.reporter.timeout.ms", "500");
                config.put("otlp.metric.reporter.jvm.metrics.enabled", "false");

                OtlpMetricReporter reporter = new OtlpMetricReporter();
                reporter.configure(config);

                MetricName name =
                        new MetricName("record-send-rate", "producer-metrics", "", Map.of("client-id", "test"));
                KafkaMetric metric = new KafkaMetric(
                        new Object(), name, (Measurable) (cfg, now) -> 1.0, new MetricConfig(), Time.SYSTEM);
                reporter.metricChange(metric);

                // Run for 5 s - 4-5 export ticks at the 1 s interval. Every
                // tick should time out at 500 ms and drop its batch; none
                // should ever reach the collector's debug exporter.
                Thread.sleep(5000);

                long closeStartedAt = System.currentTimeMillis();
                reporter.close();
                long closeMs = System.currentTimeMillis() - closeStartedAt;

                // Assertion 1: close() did not hang on the stalled export.
                // Grace window: timeout (500 ms) + 2 s headroom for the
                // scheduler's shutdown drain.
                assertTrue(
                        closeMs < 2_500L,
                        "reporter.close() took " + closeMs + " ms - scheduler stalled on a hung export");

                // Assertion 2: no batch survived the timeout. The
                // collector's debug exporter logs every received metric;
                // the Toxiproxy latency is 5 s and the reporter's timeout
                // is 500 ms, so if any 'record-send-rate' line appears,
                // a batch was delivered past its timeout.
                String collectorLogs = collector.getLogs();
                assertFalse(
                        collectorLogs.contains("record-send-rate"),
                        "collector received a metric past its 500 ms timeout - "
                                + "exporter did not drop the batch:\n"
                                + collectorLogs);
            } finally {
                if (toxiproxy != null) toxiproxy.stop();
                if (collector != null) collector.stop();
            }
        }
    }
}
