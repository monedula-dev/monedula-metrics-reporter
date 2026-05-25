// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter.e2e;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

/**
 * End-to-end test that wires together the full observability stack and proves
 * that Kafka client metrics produced by our OtlpMetricReporter plugin land in
 * Prometheus through an OpenTelemetry Collector.
 *
 * Pipeline under test:
 *   KafkaProducer (test JVM, plugin attached)
 *     -> OtlpMetricReporter (this project, shadow JAR)
 *     -> OpenTelemetry Collector (OTLP gRPC receiver, prometheus pull exporter on :8889)
 *     <- Prometheus (scrapes the collector, TSDB)
 *
 * The same plugin is also installed inside the Kafka broker container so that
 * broker-side metrics flow through the same pipeline.
 */
@Testcontainers
class KafkaOtlpE2ETest {

    /**
     * Fixed host port for Kafka's external listener. Using a fixed port avoids
     * the chicken-and-egg problem of needing to know the mapped port before
     * setting ADVERTISED_LISTENERS in the container environment.
     */
    private static final int KAFKA_HOST_PORT = 49092;

    @Test
    void kafka_producer_metrics_appear_in_prometheus() throws Exception {
        try (Network network = Network.newNetwork()) {
            GenericContainer<?> prometheus = null;
            GenericContainer<?> collector = null;
            GenericContainer<?> kafka = null;
            try {
                // --- OTLP Collector ---
                // Start the collector first because Prometheus scrapes its
                // /metrics endpoint (port 8889) via the "otel-collector"
                // network alias — that alias must exist before Prometheus
                // starts its first scrape attempt.
                //
                // We expose:
                //   4317 - OTLP gRPC receiver (Kafka broker + test JVM push to it)
                //   8889 - Prometheus /metrics endpoint (pull-based pipeline)
                collector = new GenericContainer<>("otel/opentelemetry-collector-contrib:0.152.0")
                        .withNetwork(network)
                        .withNetworkAliases("otel-collector")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("otel-collector-e2e-config.yaml"),
                                "/etc/otel-collector-config.yaml")
                        .withCommand("--config=/etc/otel-collector-config.yaml")
                        .withExposedPorts(4317, 8889)
                        .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("otel-collector")))
                        .waitingFor(Wait.forLogMessage(".*Everything is ready.*", 1));
                collector.start();

                // --- Prometheus ---
                // Scrapes the collector's prometheus exporter (port 8889).
                // This pull-based path preserves # TYPE / # HELP, so
                // /api/v1/metadata returns gauge/counter rather than "unknown".
                prometheus = new GenericContainer<>("prom/prometheus:v3.11.3")
                        .withNetwork(network)
                        .withNetworkAliases("prometheus")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("prometheus-e2e.yml"),
                                "/etc/prometheus/prometheus.yml")
                        .withCommand("--config.file=/etc/prometheus/prometheus.yml")
                        .withExposedPorts(9090)
                        .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("prometheus")))
                        .waitingFor(Wait.forHttp("/-/ready").forPort(9090));
                prometheus.start();

                // gRPC endpoint reachable from the test JVM (mapped host port)
                String collectorEndpointFromHost =
                        "http://" + collector.getHost() + ":" + collector.getMappedPort(4317);

                // --- Kafka (KRaft, apache/kafka:4.2.0) with plugin ---
                // KafkaContainer from testcontainers:kafka 1.19.8 only supports Confluent images,
                // so we use GenericContainer with apache/kafka in KRaft mode instead.
                //
                // We expose two listeners:
                //   PLAINTEXT (9092) - for internal Docker network traffic
                //   EXTERNAL  (19092) - bound to a fixed host port so the test JVM can connect
                //
                // The broker is configured with the OTLP plugin so it sends its own broker
                // metrics to the collector. The test producer is separately configured below
                // to also use the plugin.
                kafka = new GenericContainer<>("apache/kafka:4.2.0")
                        .withNetwork(network)
                        .withNetworkAliases("kafka")
                        .withEnv("KAFKA_NODE_ID", "1")
                        .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
                        .withEnv(
                                "KAFKA_LISTENERS",
                                "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,EXTERNAL://0.0.0.0:19092")
                        .withEnv(
                                "KAFKA_ADVERTISED_LISTENERS",
                                "PLAINTEXT://kafka:9092,EXTERNAL://localhost:" + KAFKA_HOST_PORT)
                        .withEnv(
                                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                                "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT")
                        .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@kafka:9093")
                        .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
                        .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
                        // Plugin in the broker - sends broker-side metrics to collector
                        // (both Kafka SPI and Yammer/Coda Hale registries — single reporter handles both).
                        .withEnv("KAFKA_METRIC_REPORTERS", "dev.monedula.metricsreporter.OtlpMetricReporter")
                        .withEnv("KAFKA_OTLP_METRIC_REPORTER_ENDPOINT", "http://otel-collector:4317")
                        .withEnv("KAFKA_OTLP_METRIC_REPORTER_TRANSPORT", "grpc")
                        .withEnv("KAFKA_OTLP_METRIC_REPORTER_INTERVAL_MS", "5000")
                        // OtlpMetricReporterConfig requires timeout.ms < interval.ms strictly.
                        // The default timeout (5000) equals our test interval, so set it lower.
                        .withEnv("KAFKA_OTLP_METRIC_REPORTER_TIMEOUT_MS", "3000")
                        .withCopyFileToContainer(
                                // Stable-name JAR produced by the quickstartShadowJar task that
                                // finalises shadowJar — avoids hard-coding the project version here.
                                // Lives in a separate build dir so it doesn't collide with the
                                // versioned shadowJar output that release publishing consumes.
                                MountableFile.forHostPath("../build/quickstart-libs/monedula-metrics-reporter.jar"),
                                "/opt/kafka/libs/monedula-metrics-reporter.jar")
                        .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("kafka")))
                        .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1));
                kafka.setPortBindings(List.of(KAFKA_HOST_PORT + ":19092"));
                kafka.start();

                // --- Produce messages to generate producer-side metrics ---
                // The KafkaProducer is also configured with our OTLP metric reporter so that
                // producer-client metrics (like producer-metrics.record-send-rate) are exported
                // via OTLP to the collector and ultimately appear in Prometheus.
                Properties props = new Properties();
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + KAFKA_HOST_PORT);
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                // Attach our plugin to the test-JVM producer
                props.put(
                        ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                        "dev.monedula.metricsreporter.OtlpMetricReporter");
                props.put("otlp.metric.reporter.endpoint", collectorEndpointFromHost);
                props.put("otlp.metric.reporter.transport", "grpc");
                props.put("otlp.metric.reporter.interval.ms", "2000");
                // OtlpMetricReporterConfig enforces timeout.ms < interval.ms so the
                // scheduler always has headroom to start the next tick. We tighten
                // interval.ms to 2000 to get metrics out fast; the default timeout
                // (5000) would now exceed that, so set timeout below interval.
                props.put("otlp.metric.reporter.timeout.ms", "1500");

                try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                    for (int i = 0; i < 20; i++) {
                        producer.send(new ProducerRecord<>("test-topic", "key-" + i, "value-" + i));
                    }
                    producer.flush();
                    // Hold the producer open long enough for at least 2 export ticks (2s interval)
                    Thread.sleep(10_000);
                }

                // --- Assert producer metric appears in Prometheus ---
                // Task A switched to empty units so Prometheus no longer appends "_ratio";
                // we still accept both names for forward/backward tolerance.
                String prometheusUrl = "http://" + prometheus.getHost() + ":" + prometheus.getMappedPort(9090);
                String[] producerCandidates = {
                    // The test-JVM producer uses our reporter, and Kafka sets
                    // _namespace="kafka.producer" on the client side, so the SPI
                    // metric name carries a kafka_producer_ prefix.
                    "kafka_producer_producer_metrics_record_send_rate",
                    "kafka_producer_producer_metrics_record_send_rate_ratio",
                    // Broker-side SPI prefix (in case the metric is sourced there).
                    "kafka_server_producer_metrics_record_send_rate",
                    "kafka_server_producer_metrics_record_send_rate_ratio",
                    // legacy fallbacks (unlikely after this change but harmless)
                    "producer_metrics_record_send_rate",
                    "producer_metrics_record_send_rate_ratio"
                };
                assertTrue(
                        awaitAnyMetric(prometheusUrl, producerCandidates, Duration.ofSeconds(30)),
                        "Expected one of " + Arrays.toString(producerCandidates) + " in Prometheus");

                // --- Assert broker-side Yammer metric appears in Prometheus ---
                // ActiveControllerCount is always exactly 1 on a healthy single-node KRaft
                // broker, so it's the safest broker-internal Yammer metric to assert on.
                assertTrue(
                        awaitAnyMetric(
                                prometheusUrl,
                                new String[] {"kafka_controller_kafkacontroller_activecontrollercount"},
                                Duration.ofSeconds(30)),
                        "Expected kafka_controller_kafkacontroller_activecontrollercount "
                                + "(broker Yammer metric) in Prometheus");

                // --- Assert per-partition Yammer metric carries topic/partition labels ---
                // Once a partition has been assigned a leader, Kafka registers the per-partition
                // UnderReplicated gauge with scope="topic.<topic>.partition.<n>". We produced to
                // test-topic above which auto-creates it and triggers leader assignment.
                assertTrue(
                        awaitLabeledMetric(
                                prometheusUrl,
                                "kafka_cluster_partition_underreplicated{topic=\"test-topic\"}",
                                Duration.ofSeconds(60)),
                        "Expected kafka_cluster_partition_underreplicated{topic=\"test-topic\"} "
                                + "(per-partition Yammer metric with topic label) in Prometheus");

                // --- Assert broker-context label is attached to broker-side metrics ---
                // Kafka's MetricsContext exposes the broker identity. In KRaft mode (which
                // we run here) the context key is "kafka.node.id" — older ZK-based brokers
                // used "kafka.broker.id". After OTel/Prometheus dotted-key sanitization the
                // label becomes "kafka_node_id".
                assertTrue(
                        awaitLabeledMetric(
                                prometheusUrl,
                                "kafka_controller_kafkacontroller_activecontrollercount{kafka_node_id=\"1\"}",
                                Duration.ofSeconds(30)),
                        "Expected kafka_controller_kafkacontroller_activecontrollercount "
                                + "to carry a kafka_node_id=\"1\" label sourced from MetricsContext");

                // --- Assert JVM runtime metric appears in Prometheus ---
                // RuntimeMetrics from opentelemetry-runtime-telemetry-java17 emits
                // metrics like jvm.thread.count / jvm.memory.used. The collector's
                // `prometheus` (pull) exporter sanitizes dots to underscores and may
                // append unit suffixes, so we try several variants.
                String[] jvmCandidates = {
                    "jvm_thread_count",
                    "jvm_threads_count",
                    "jvm_memory_used_bytes",
                    "jvm_memory_used",
                    "jvm_class_count",
                    "jvm_classes_count",
                    "jvm_cpu_count",
                };
                assertTrue(
                        awaitAnyMetric(prometheusUrl, jvmCandidates, Duration.ofSeconds(30)),
                        "Expected one of " + Arrays.toString(jvmCandidates) + " in Prometheus");

                // --- Assert reporter self-monitoring metrics reach Prometheus ---
                // MetricCollector emits three reporter-health metrics on every
                // tick: success/failure counters and a duration gauge. They use
                // the same OTLP pipeline as Kafka metrics, so if these don't
                // arrive, neither would anything else - but a dedicated
                // assertion makes it clear when the self-monitoring path itself
                // regresses.
                String[] selfMetricCandidates = {
                    "monedula_reporter_export_success_total",
                    "monedula_reporter_export_success",
                    "monedula_reporter_export_failure_total",
                    "monedula_reporter_export_failure",
                    "monedula_reporter_export_duration_ms",
                };
                assertTrue(
                        awaitAnyMetric(prometheusUrl, selfMetricCandidates, Duration.ofSeconds(30)),
                        "Expected one of " + Arrays.toString(selfMetricCandidates)
                                + " (reporter self-monitoring) in Prometheus");

                // --- Assert metric *type* metadata flows end-to-end ---
                // With the pull-based pipeline (collector's `prometheus` exporter
                // on :8889 scraped by Prometheus), # TYPE / # HELP comments are
                // preserved, so /api/v1/metadata?metric=<name> returns the
                // metric's real type ("gauge" for ActiveControllerCount), not
                // "unknown".
                HttpClient mdClient = HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(5))
                        .build();
                HttpRequest mdRequest = HttpRequest.newBuilder()
                        .uri(URI.create(prometheusUrl
                                + "/api/v1/metadata?metric=kafka_controller_kafkacontroller_activecontrollercount"))
                        .timeout(Duration.ofSeconds(10))
                        .build();
                HttpResponse<String> mdResponse = mdClient.send(mdRequest, HttpResponse.BodyHandlers.ofString());
                assertTrue(
                        mdResponse.body().contains("\"type\":\"gauge\""),
                        "Expected kafka_controller_kafkacontroller_activecontrollercount "
                                + "to have type=gauge metadata, got: " + mdResponse.body());
            } finally {
                if (kafka != null) kafka.stop();
                if (prometheus != null) prometheus.stop();
                if (collector != null) collector.stop();
            }
        }
    }

    /**
     * Poll Prometheus until any of {@code metricNames} returns a non-empty instant-query
     * result, or {@code timeout} elapses. Returns true on first hit, false if the deadline
     * passes. Wait between polls is short (1s) so a positive answer surfaces quickly.
     */
    private boolean awaitAnyMetric(String baseUrl, String[] metricNames, Duration timeout)
            throws IOException, InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            for (String name : metricNames) {
                if (metricExistsInPrometheus(baseUrl, name)) return true;
            }
            Thread.sleep(1_000);
        }
        return false;
    }

    /** Same as {@link #awaitAnyMetric} but for a single PromQL selector with label matchers. */
    private boolean awaitLabeledMetric(String baseUrl, String promQL, Duration timeout)
            throws IOException, InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (labeledMetricExistsInPrometheus(baseUrl, promQL)) return true;
            Thread.sleep(1_000);
        }
        return false;
    }

    private boolean metricExistsInPrometheus(String baseUrl, String metricName)
            throws IOException, InterruptedException {
        HttpClient client =
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/query?query=" + metricName))
                .timeout(Duration.ofSeconds(10))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return response.statusCode() == 200 && response.body().contains("\"result\":[{");
    }

    private boolean labeledMetricExistsInPrometheus(String baseUrl, String promQL)
            throws IOException, InterruptedException {
        HttpClient client =
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        String encoded = java.net.URLEncoder.encode(promQL, java.nio.charset.StandardCharsets.UTF_8);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v1/query?query=" + encoded))
                .timeout(Duration.ofSeconds(10))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return response.statusCode() == 200 && response.body().contains("\"result\":[{");
    }
}
