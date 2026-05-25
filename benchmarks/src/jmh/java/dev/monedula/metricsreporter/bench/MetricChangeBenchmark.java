// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter.bench;

import dev.monedula.metricsreporter.OtlpMetricReporter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.utils.Time;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures OtlpMetricReporter.metricChange() on the calling thread.
 *
 * <p>Backs the "callbacks are cheap, no I/O on the calling thread" claim in
 * docs/architecture.md and docs/assumptions.md.
 *
 * <p>The reporter is fully configured at @Setup time pointing at 127.0.0.1:1
 * (TCP-refused, no DNS lookup) with JVM metrics off, so the background export
 * scheduler's failures stay off the measurement window.
 */
@BenchmarkMode({Mode.AverageTime, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class MetricChangeBenchmark {

    private static final int POOL_SIZE = 1000;

    @Param({"none", "single-pattern", "five-patterns"})
    public String allowList;

    private OtlpMetricReporter reporter;
    private KafkaMetric[] pool;

    @Setup(Level.Trial)
    public void setup() {
        Map<String, String> config = new HashMap<>();
        config.put("otlp.metric.reporter.endpoint", "http://127.0.0.1:1");
        config.put("otlp.metric.reporter.interval.ms", "60000");
        config.put("otlp.metric.reporter.timeout.ms", "1000");
        config.put("otlp.metric.reporter.jvm.metrics.enabled", "false");
        switch (allowList) {
            case "single-pattern":
                config.put("otlp.metric.reporter.allowed.metrics", "producer-metrics\\..*");
                break;
            case "five-patterns":
                config.put(
                        "otlp.metric.reporter.allowed.metrics",
                        "producer-metrics\\..*,consumer-metrics\\..*,"
                                + "broker-metrics\\..*,jvm-metrics\\..*,kafka-metrics\\..*");
                break;
            default:
                // "none" — no allow-list configured (allow-all path)
        }
        reporter = new OtlpMetricReporter();
        reporter.configure(config);

        // Real KafkaMetric instances rather than Mockito mocks. Mockito's
        // ByteBuddy proxy adds ~15 us per metricName() call (verified
        // empirically), which would dominate the measurement window and
        // mask the actual hot-path cost. The 5-arg KafkaMetric constructor
        // is public in Kafka 4.x; we use a constant Measurable so the
        // valueProvider call is also cheap.
        MetricConfig metricConfig = new MetricConfig();
        Measurable constantOne = (cfg, now) -> 1.0;
        pool = new KafkaMetric[POOL_SIZE];
        for (int i = 0; i < POOL_SIZE; i++) {
            MetricName name = new MetricName("metric-" + i, "producer-metrics", "", Map.of("id", String.valueOf(i)));
            pool[i] = new KafkaMetric(new Object(), name, constantOne, metricConfig, Time.SYSTEM);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        reporter.close();
    }

    @Benchmark
    @Threads(1)
    public void singleThread() {
        reporter.metricChange(pool[ThreadLocalRandom.current().nextInt(POOL_SIZE)]);
    }

    @Benchmark
    @Threads(4)
    public void fourThreads() {
        reporter.metricChange(pool[ThreadLocalRandom.current().nextInt(POOL_SIZE)]);
    }

    @Benchmark
    @Threads(16)
    public void sixteenThreads() {
        reporter.metricChange(pool[ThreadLocalRandom.current().nextInt(POOL_SIZE)]);
    }
}
