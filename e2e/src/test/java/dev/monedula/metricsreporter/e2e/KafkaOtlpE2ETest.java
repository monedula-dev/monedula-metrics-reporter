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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringDeserializer;
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
     * setting ADVERTISED_LISTENERS in the container environment. Shared by both
     * tests — safe because JUnit runs the methods of this class sequentially;
     * enabling parallel execution would require distinct ports per test.
     */
    private static final int KAFKA_HOST_PORT = 49092;

    /** The three-container observability stack shared by both tests. */
    private record E2eStack(GenericContainer<?> collector, GenericContainer<?> prometheus, GenericContainer<?> kafka) {

        String prometheusUrl() {
            return "http://" + prometheus.getHost() + ":" + prometheus.getMappedPort(9090);
        }

        /** OTLP gRPC endpoint reachable from the test JVM (mapped host port). */
        String collectorGrpcEndpointFromHost() {
            return "http://" + collector.getHost() + ":" + collector.getMappedPort(4317);
        }

        void stopAll() {
            if (kafka != null) kafka.stop();
            if (prometheus != null) prometheus.stop();
            if (collector != null) collector.stop();
        }
    }

    /**
     * Start the collector → Prometheus → Kafka(KRaft + plugin) stack on {@code network}.
     * The container configuration is deliberately identical for every test in this class —
     * keep changes here so the tests can't drift apart; {@code logSuffix} only disambiguates
     * the per-test log-consumer names. Containers that already started are stopped again if
     * a later one fails to start.
     *
     * <p>Collector first, because Prometheus scrapes its /metrics endpoint (port 8889) via the
     * "otel-collector" network alias — that alias must exist before Prometheus's first scrape.
     * Exposed: 4317 (OTLP gRPC receiver), 8889 (Prometheus pull endpoint). The pull-based path
     * preserves # TYPE / # HELP, so /api/v1/metadata returns real types rather than "unknown".
     *
     * <p>Kafka: KafkaContainer from testcontainers only supports Confluent images, so we use
     * GenericContainer with apache/kafka in KRaft mode. Two listeners: PLAINTEXT (9092) for
     * internal Docker network traffic, EXTERNAL (19092) bound to {@link #KAFKA_HOST_PORT} so the
     * test JVM can connect. The broker carries the OTLP plugin (stable-name JAR produced by the
     * quickstartShadowJar task, avoiding a hard-coded version). timeout.ms is set below the 5s
     * interval because OtlpMetricReporterConfig requires timeout.ms < interval.ms strictly.
     */
    private static E2eStack startStack(Network network, String logSuffix) {
        return startStack(network, logSuffix, Map.of());
    }

    /**
     * Overload that layers {@code extraKafkaEnv} on top of the standard Kafka container environment.
     * The extras are applied with {@code withEnv} AFTER the standard entries, so a duplicate key in
     * {@code extraKafkaEnv} wins (Testcontainers/last-write-wins). Values pass through to Docker
     * literally — there is no shell, so a backslash in a regex (e.g. {@code producer-metrics\..*})
     * reaches the broker config unescaped. Used by the dynamic-reconfiguration test to start the
     * broker with a restrictive {@code KAFKA_OTLP_METRIC_REPORTER_ALLOWED_METRICS}.
     */
    private static E2eStack startStack(Network network, String logSuffix, Map<String, String> extraKafkaEnv) {
        GenericContainer<?> collector = null;
        GenericContainer<?> prometheus = null;
        GenericContainer<?> kafka = null;
        try {
            collector = new GenericContainer<>("otel/opentelemetry-collector-contrib:0.152.0")
                    .withNetwork(network)
                    .withNetworkAliases("otel-collector")
                    .withCopyFileToContainer(
                            MountableFile.forClasspathResource("otel-collector-e2e-config.yaml"),
                            "/etc/otel-collector-config.yaml")
                    .withCommand("--config=/etc/otel-collector-config.yaml")
                    .withExposedPorts(4317, 8889)
                    .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("otel-collector" + logSuffix)))
                    .waitingFor(Wait.forLogMessage(".*Everything is ready.*", 1));
            collector.start();

            prometheus = new GenericContainer<>("prom/prometheus:v3.11.3")
                    .withNetwork(network)
                    .withNetworkAliases("prometheus")
                    .withCopyFileToContainer(
                            MountableFile.forClasspathResource("prometheus-e2e.yml"), "/etc/prometheus/prometheus.yml")
                    .withCommand("--config.file=/etc/prometheus/prometheus.yml")
                    .withExposedPorts(9090)
                    .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("prometheus" + logSuffix)))
                    .waitingFor(Wait.forHttp("/-/ready").forPort(9090));
            prometheus.start();

            kafka = new GenericContainer<>("apache/kafka:4.2.1")
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
                    // Plugin in the broker: exports broker-side metrics (SPI + Yammer) and receives
                    // KIP-714 client pushes, all forwarded to the collector over OTLP.
                    .withEnv("KAFKA_METRIC_REPORTERS", "dev.monedula.metricsreporter.OtlpMetricReporter")
                    .withEnv("KAFKA_OTLP_METRIC_REPORTER_ENDPOINT", "http://otel-collector:4317")
                    .withEnv("KAFKA_OTLP_METRIC_REPORTER_TRANSPORT", "grpc")
                    .withEnv("KAFKA_OTLP_METRIC_REPORTER_INTERVAL_MS", "5000")
                    .withEnv("KAFKA_OTLP_METRIC_REPORTER_TIMEOUT_MS", "3000")
                    .withCopyFileToContainer(
                            MountableFile.forHostPath("../build/quickstart-libs/monedula-metrics-reporter.jar"),
                            "/opt/kafka/libs/monedula-metrics-reporter.jar")
                    .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("kafka" + logSuffix)))
                    .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1));
            // Per-test env overrides, applied last so they win over the standard entries above.
            for (Map.Entry<String, String> e : extraKafkaEnv.entrySet()) {
                kafka.withEnv(e.getKey(), e.getValue());
            }
            kafka.setPortBindings(List.of(KAFKA_HOST_PORT + ":19092"));
            kafka.start();

            return new E2eStack(collector, prometheus, kafka);
        } catch (RuntimeException e) {
            new E2eStack(collector, prometheus, kafka).stopAll();
            throw e;
        }
    }

    @Test
    void kafka_producer_metrics_appear_in_prometheus() throws Exception {
        try (Network network = Network.newNetwork()) {
            E2eStack stack = startStack(network, "");
            try {
                String collectorEndpointFromHost = stack.collectorGrpcEndpointFromHost();

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
                String prometheusUrl = stack.prometheusUrl();
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
                stack.stopAll();
            }
        }
    }

    /**
     * End-to-end test for KIP-714 client telemetry: a producer with
     * {@code enable.metrics.push=true} pushes its own OTLP telemetry to the broker via
     * the KIP-714 PushTelemetry RPC. The broker's plugin receiver forwards each payload to
     * the OTel collector (also used by the existing test). The enriched series must arrive
     * in Prometheus carrying the {@code kafka_cluster_id} label added by the broker's
     * ClientMetricsEnricher.
     *
     * <p>Pipeline under test:
     *   KafkaProducer (KIP-714 push, no OtlpMetricReporter on the client side)
     *     → broker ClientTelemetryReceiverImpl (our plugin, broker side)
     *     → ClientTelemetryForwarder → OtlpExporter
     *     → OpenTelemetry Collector (OTLP gRPC, prometheus pull exporter on :8889)
     *     ← Prometheus (scrapes collector)
     */
    @Test
    void kip714_client_telemetry_lands_in_prometheus_with_cluster_id_label() throws Exception {
        try (Network network = Network.newNetwork()) {
            // Same stack as the producer test; the broker has client telemetry enabled by default
            // (CLIENT_TELEMETRY_ENABLED=true). The plugin's ClientTelemetryReceiverImpl sits idle
            // until a subscription is created; after that Kafka tells each connecting client what
            // to push and how often.
            E2eStack stack = startStack(network, "-kip714");
            try {
                // --- Step 1: Create a client-metrics subscription via AdminClient ---
                // This is what activates KIP-714 on the broker: without a subscription the broker
                // returns an empty GetTelemetrySubscriptions response and clients don't push.
                // ConfigResource.Type.CLIENT_METRICS is available since Kafka 3.7 (KIP-714).
                //
                // The "metrics" value MUST be "*" to subscribe to ALL client metrics. Kafka 4.2.1's
                // ClientMetricsConfigs defines ALL_SUBSCRIBED_METRICS = "*" and METRICS_DEFAULT =
                // List.of() (empty). An empty string therefore selects NO metrics, so clients
                // complete the GetTelemetrySubscriptions handshake but never push — which is exactly
                // the false-positive trap an earlier version of this test fell into.
                Properties adminProps = new Properties();
                adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + KAFKA_HOST_PORT);
                ConfigResource sub = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, "e2e-sub");
                try (Admin admin = Admin.create(adminProps)) {
                    Collection<AlterConfigOp> ops = List.of(
                            // "*" = subscribe to ALL client metrics (ClientMetricsConfigs.ALL_SUBSCRIBED_METRICS).
                            new AlterConfigOp(new ConfigEntry("metrics", "*"), AlterConfigOp.OpType.SET),
                            // Push every 5 seconds; clients push on this interval once subscribed.
                            new AlterConfigOp(new ConfigEntry("interval.ms", "5000"), AlterConfigOp.OpType.SET));
                    admin.incrementalAlterConfigs(Map.of(sub, ops))
                            .all()
                            .get(30, java.util.concurrent.TimeUnit.SECONDS);

                    // Confirm the subscription is fully applied BEFORE clients connect: describe the
                    // CLIENT_METRICS resource and assert it shows our values. This rules out a race
                    // where clients do GetTelemetrySubscriptions before the config has propagated.
                    Config applied = admin.describeConfigs(List.of(sub))
                            .all()
                            .get(30, java.util.concurrent.TimeUnit.SECONDS)
                            .get(sub);
                    ConfigEntry metricsEntry = applied.get("metrics");
                    ConfigEntry intervalEntry = applied.get("interval.ms");
                    assertTrue(
                            metricsEntry != null && "*".equals(metricsEntry.value()),
                            "Expected CLIENT_METRICS subscription 'metrics' = '*', got: " + metricsEntry);
                    assertTrue(
                            intervalEntry != null && "5000".equals(intervalEntry.value()),
                            "Expected CLIENT_METRICS subscription 'interval.ms' = '5000', got: " + intervalEntry);
                }

                // --- Step 2: Run a producer and consumer with KIP-714 push enabled ---
                // These are plain Kafka clients — no OtlpMetricReporter on the client side.
                // They push their OTLP telemetry to the broker via PushTelemetry, which our
                // plugin receives. enable.metrics.push is true by default, set explicitly for clarity.
                Properties producerProps = new Properties();
                producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + KAFKA_HOST_PORT);
                producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                producerProps.put("enable.metrics.push", "true");

                Properties consumerProps = new Properties();
                consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + KAFKA_HOST_PORT);
                consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "e2e-kip714-group");
                consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                consumerProps.put("enable.metrics.push", "true");

                // Keep both clients alive comfortably longer than interval.ms (5s) — produce/consume
                // real traffic for ~30s so each client refreshes subscriptions, accumulates metrics,
                // and performs several PushTelemetry calls. We poll in a loop rather than sleeping so
                // the clients stay active (heartbeats, fetches) the whole time.
                String prometheusUrl = stack.prometheusUrl();
                try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
                        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
                    consumer.subscribe(List.of("kip714-test-topic"));
                    long kip714DeadlineMs = System.currentTimeMillis() + 30_000;
                    int sent = 0;
                    while (System.currentTimeMillis() < kip714DeadlineMs) {
                        producer.send(new ProducerRecord<>("kip714-test-topic", "k-" + sent, "v-" + sent));
                        sent++;
                        consumer.poll(Duration.ofMillis(500));
                    }
                    producer.flush();
                }

                // --- Step 3a (PRIMARY proof): our forwarder actually forwarded a client push ---
                // monedula_reporter_clienttelemetry_forwarded_total is emitted by OUR
                // ClientTelemetryForwarder and is only nonzero if our broker-side
                // ClientTelemetryReceiverImpl received a client payload AND the forwarder exported it.
                // This is the unambiguous gate: it would stay 0 if the receiver were deleted. An
                // earlier version of this test passed without this gate (a broker SPI series happened
                // to carry kafka_cluster_id), so this assertion is what makes the test a TRUE proof.
                assertTrue(
                        awaitLabeledMetric(
                                prometheusUrl,
                                "monedula_reporter_clienttelemetry_forwarded_total > 0",
                                Duration.ofSeconds(60)),
                        "Expected monedula_reporter_clienttelemetry_forwarded_total > 0 — our KIP-714 "
                                + "receiver/forwarder never forwarded a client telemetry payload (clients "
                                + "are not pushing, or the receiver is broken)");

                // --- Step 3b (enrichment proof): a CLIENT-ONLY series carries the broker cluster id ---
                // The KIP-714 client pushes metrics under the org.apache.kafka.producer.* and
                // org.apache.kafka.consumer.* namespaces (verified against the live collector at
                // detailed verbosity). The broker's own SPI metrics only ever use the
                // org.apache.kafka.server.* namespace, so org_apache_kafka_(producer|consumer)_*
                // series can ONLY come from a client push through our forwarder — there is no broker
                // series that matches. Requiring kafka_cluster_id!="" on top proves our enricher
                // attached the broker identity to the client-originated series.
                //
                // (We deliberately do NOT key on client_instance_id: the client does not place it in
                // the OTLP resource attributes our converter sees — the pushed resource only carries
                // the broker enrichment our plugin adds — so the namespace split is the reliable
                // client/broker discriminator here.)
                assertTrue(
                        awaitLabeledMetric(
                                prometheusUrl,
                                "{__name__=~\"org_apache_kafka_(producer|consumer)_.*\",kafka_cluster_id!=\"\"}",
                                Duration.ofSeconds(60)),
                        "Expected at least one client-originated org_apache_kafka_(producer|consumer)_* "
                                + "series (a namespace only KIP-714 clients emit, never the broker) carrying "
                                + "kafka_cluster_id — proves a client push reached Prometheus with broker enrichment");
            } finally {
                stack.stopAll();
            }
        }
    }

    /**
     * End-to-end proof that {@code otlp.metric.reporter.allowed.metrics} can be widened LIVE via
     * Kafka's KIP-226 dynamic broker configuration — no broker restart — and that a metric which
     * was stored-but-filtered resurfaces in Prometheus on the next export tick.
     *
     * <p>Why this is the true proof of the feature: filtering moved from ingest to export time
     * (commit "Move allow-list filtering from ingest to export time"), so the broker's registries
     * always hold every metric Kafka announces once at {@code init()}; only the export-time
     * {@link dev.monedula.metricsreporter.AllowList} decides what ships. Widening that allow-list at
     * runtime therefore RESURRECTS a previously-excluded metric that ingest-time filtering could
     * never have recovered (Kafka never re-announces most SPI/Yammer metrics).
     *
     * <p>Pipeline under test:
     *   broker OtlpMetricReporter (implements Reconfigurable)
     *     -> MetricCollector applies the volatile AllowList between snapshot and mapping
     *     -> OpenTelemetry Collector (OTLP gRPC, prometheus pull exporter on :8889)
     *     <- Prometheus (scrapes the collector)
     *   AdminClient.incrementalAlterConfigs(BROKER "1", allowed.metrics="") -> Kafka -> reconfigure()
     *
     * <p>Scenario:
     * <ol>
     *   <li>Start the broker with {@code KAFKA_OTLP_METRIC_REPORTER_ALLOWED_METRICS=producer-metrics\..*}.
     *       That single pattern admits SPI subjects like {@code producer-metrics.record-send-rate}
     *       ({@code {group}.{name}}) but NOT the Yammer controller subject
     *       {@code kafka.controller.KafkaController.ActiveControllerCount} ({@code {group}.{type}.{name}}),
     *       so {@code kafka_controller_kafkacontroller_activecontrollercount} must be ABSENT.</li>
     *   <li>Liveness gate: await {@code monedula_reporter_export_success_total}. MetricCollector emits
     *       the self-monitoring metrics AFTER the allow-list filter (see exportTick), so their arrival
     *       proves the whole export pipeline is up regardless of the allow-list.</li>
     *   <li>Negative check: with the pipeline demonstrably exporting (and one extra interval elapsed),
     *       assert the Yammer controller metric is absent — if it were going to ship under this
     *       allow-list it already would have.</li>
     *   <li>Widen live: {@code incrementalAlterConfigs} sets {@code allowed.metrics} to "" (empty =
     *       allow-all, the documented semantics) on the BROKER resource. No restart.</li>
     *   <li>Await the previously-absent {@code kafka_controller_kafkacontroller_activecontrollercount}
     *       — the resurrect proof.</li>
     * </ol>
     *
     * <p>This also guards the design assumption that Kafka delivers the COMPLETE merged config map to
     * {@code reconfigure()}/{@code validateReconfiguration()}: those construct a fresh
     * {@code OtlpMetricReporterConfig(configs)}, which requires all keys (including static ones with
     * defaults). If Kafka passed only the changed key, that construction would throw and the
     * {@code --alter} would be REJECTED — so a passing widen is itself the proof of the full-map
     * contract.
     */
    @Test
    void allow_list_widens_live_via_dynamic_broker_config() throws Exception {
        try (Network network = Network.newNetwork()) {
            // Restrictive allow-list: admits some SPI producer metrics, excludes the Yammer controller
            // metric. Single pattern (NO commas — commas split patterns). The backslash reaches the
            // broker config literally through withEnv (no shell), giving regex "producer-metrics\..*".
            E2eStack stack = startStack(
                    network,
                    "-reconfig",
                    Map.of("KAFKA_OTLP_METRIC_REPORTER_ALLOWED_METRICS", "producer-metrics\\..*"));
            try {
                String prometheusUrl = stack.prometheusUrl();

                // --- Positive control (liveness gate) ---
                // Self-monitoring metrics are added AFTER the allow-list filter in exportTick, so they
                // ship no matter how restrictive the allow-list is. Their arrival proves the broker's
                // reporter -> collector -> Prometheus path is fully up before we assert an absence.
                assertTrue(
                        awaitAnyMetric(
                                prometheusUrl,
                                new String[] {
                                    "monedula_reporter_export_success_total", "monedula_reporter_export_success"
                                },
                                Duration.ofSeconds(60)),
                        "Expected monedula_reporter_export_success_total (self-monitoring, bypasses the "
                                + "allow-list) in Prometheus — the export pipeline must be up before the absence check");

                // Give one extra full export interval (broker interval.ms=5000) so that, if the Yammer
                // controller metric were admissible under the current allow-list, it would already have
                // been exported and scraped. It is NOT admissible, so this makes the absence reliable.
                Thread.sleep(8_000);

                // --- Negative check: the excluded Yammer metric must be ABSENT under the narrow list ---
                // Yammer subject is {group}.{type}.{name} = kafka.controller.KafkaController.ActiveControllerCount,
                // which "producer-metrics\..*" (anchored whole-subject match) cannot match, so the export
                // filter drops it even though the registry holds it.
                assertTrue(
                        metricAbsentFromPrometheus(
                                prometheusUrl, "kafka_controller_kafkacontroller_activecontrollercount"),
                        "Expected kafka_controller_kafkacontroller_activecontrollercount to be ABSENT under the "
                                + "restrictive allow-list (producer-metrics\\..* excludes the Yammer controller subject)");

                // --- Widen the allow-list LIVE via dynamic broker config (KIP-226), no restart ---
                // Empty string = allow-all (OtlpMetricReporterConfig.compilePatterns treats blank as an
                // empty pattern list). Applied to the per-broker resource (node id "1").
                Properties adminProps = new Properties();
                adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + KAFKA_HOST_PORT);
                ConfigResource broker = new ConfigResource(ConfigResource.Type.BROKER, "1");
                try (Admin admin = Admin.create(adminProps)) {
                    Collection<AlterConfigOp> ops = List.of(new AlterConfigOp(
                            new ConfigEntry("otlp.metric.reporter.allowed.metrics", ""), AlterConfigOp.OpType.SET));
                    admin.incrementalAlterConfigs(Map.of(broker, ops))
                            .all()
                            .get(30, java.util.concurrent.TimeUnit.SECONDS);

                    // Confirm the alter was applied and persisted as a dynamic broker config. We assert
                    // PRESENCE + source, not the value: the broker's own ConfigDef does not know the
                    // reporter's custom keys, so describeConfigs classifies them as sensitive and REDACTS
                    // the value (isSensitive=true, value="Redacted"). The redaction is only in the admin
                    // read-back — the broker still delivers the real value to the plugin's reconfigure().
                    // (This is why the resurrect assertion below is the actual proof the value took effect.)
                    Config applied = admin.describeConfigs(List.of(broker))
                            .all()
                            .get(30, java.util.concurrent.TimeUnit.SECONDS)
                            .get(broker);
                    ConfigEntry allowedEntry = applied.get("otlp.metric.reporter.allowed.metrics");
                    assertTrue(
                            allowedEntry != null
                                    && allowedEntry.source() == ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG,
                            "Expected 'otlp.metric.reporter.allowed.metrics' to be set as a DYNAMIC_BROKER_CONFIG "
                                    + "after the alter (value is redacted by the broker for unknown keys), got: "
                                    + allowedEntry);
                }

                // --- Resurrect proof: the previously-absent Yammer metric now appears ---
                // No broker restart occurred; reconfigure() hot-swapped the AllowList and the next export
                // tick shipped the controller metric the registry was holding all along. A passing await
                // here also proves Kafka delivered the FULL merged config map to reconfigure (a partial
                // map would have failed OtlpMetricReporterConfig construction and rejected the alter).
                assertTrue(
                        awaitAnyMetric(
                                prometheusUrl,
                                new String[] {"kafka_controller_kafkacontroller_activecontrollercount"},
                                Duration.ofSeconds(60)),
                        "Expected kafka_controller_kafkacontroller_activecontrollercount to APPEAR after widening "
                                + "the allow-list live via dynamic broker config — the resurrect proof (no restart)");
            } finally {
                stack.stopAll();
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

    /**
     * Single point-in-time check that {@code metricName} returns an EMPTY instant-query result.
     * Used for the absence assertion, which is only meaningful once the pipeline is proven live and
     * an extra export interval has elapsed (see the caller) — otherwise "absent" could just mean
     * "not exported yet".
     */
    private boolean metricAbsentFromPrometheus(String baseUrl, String metricName)
            throws IOException, InterruptedException {
        return !metricExistsInPrometheus(baseUrl, metricName);
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
