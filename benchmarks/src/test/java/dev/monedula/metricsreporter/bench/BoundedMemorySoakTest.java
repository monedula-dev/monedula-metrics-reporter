// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter.bench;

import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.monedula.metricsreporter.OtlpMetricReporter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.Test;

/**
 * Drives OtlpMetricReporter against a dead collector for 5 minutes while
 * sampling heap usage every 5 s. Writes a CSV of the heap trace and asserts
 * that the peak heap in the last minute is within 100 MB of the post-warmup
 * baseline.
 *
 * <p>Backs the "no retry queue, bounded memory under a dead collector" claim
 * in docs/assumptions.md and README.md.
 *
 * <p>The 100 MB threshold is deliberately generous: it's high enough that
 * JIT noise and per-iteration allocation churn cannot false-alarm, low
 * enough to catch a genuine unbounded growth.
 */
class BoundedMemorySoakTest {

    private static final int POOL_SIZE = 1000;
    private static final long SOAK_DURATION_MS = 5 * 60 * 1000L;
    private static final long SAMPLE_INTERVAL_MS = 5_000L;
    private static final long BASELINE_WINDOW_START_MS = 30_000L;
    private static final long BASELINE_WINDOW_END_MS = 60_000L;
    private static final long PEAK_WINDOW_START_MS = 240_000L;
    private static final long PEAK_WINDOW_END_MS = 300_000L;
    private static final long MAX_GROWTH_BYTES = 100L * 1024 * 1024;

    @Test
    void heap_stays_bounded_against_dead_collector() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("otlp.metric.reporter.endpoint", "http://127.0.0.1:1");
        config.put("otlp.metric.reporter.interval.ms", "200");
        config.put("otlp.metric.reporter.timeout.ms", "100");
        config.put("otlp.metric.reporter.jvm.metrics.enabled", "false");

        OtlpMetricReporter reporter = new OtlpMetricReporter();
        reporter.configure(config);

        // Real KafkaMetric instances rather than Mockito mocks. Mockito records
        // every invocation on every mock for later verification; a 5-minute soak
        // doing millions of metricName() calls accumulates hundreds of MB of
        // recorded invocations and OOMs the test JVM. We don't need verification
        // here - only a stable metric pool. The 5-arg KafkaMetric constructor is
        // public in Kafka 4.x.
        MetricConfig metricConfig = new MetricConfig();
        Measurable constantOne = (cfg, now) -> 1.0;
        KafkaMetric[] pool = new KafkaMetric[POOL_SIZE];
        for (int i = 0; i < POOL_SIZE; i++) {
            MetricName name = new MetricName("metric-" + i, "producer-metrics", "", Map.of("id", String.valueOf(i)));
            pool[i] = new KafkaMetric(new Object(), name, constantOne, metricConfig, Time.SYSTEM);
        }

        long testStart = System.currentTimeMillis();
        AtomicBoolean keepRunning = new AtomicBoolean(true);
        CountDownLatch driverStopped = new CountDownLatch(1);
        CountDownLatch samplerStopped = new CountDownLatch(1);
        List<Sample> samples = new ArrayList<>();

        Thread driver = new Thread(
                () -> {
                    try {
                        while (keepRunning.get()) {
                            reporter.metricChange(
                                    pool[ThreadLocalRandom.current().nextInt(POOL_SIZE)]);
                            Thread.sleep(1);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        driverStopped.countDown();
                    }
                },
                "soak-driver");
        driver.setDaemon(true);

        Thread sampler = new Thread(
                () -> {
                    try {
                        long nextSampleAt = testStart + SAMPLE_INTERVAL_MS;
                        while (keepRunning.get()) {
                            long now = System.currentTimeMillis();
                            if (now >= nextSampleAt) {
                                System.gc();
                                long heapRuntime = Runtime.getRuntime().totalMemory()
                                        - Runtime.getRuntime().freeMemory();
                                long heapMxbean = ManagementFactory.getMemoryMXBean()
                                        .getHeapMemoryUsage()
                                        .getUsed();
                                synchronized (samples) {
                                    samples.add(new Sample(now - testStart, heapRuntime, heapMxbean));
                                }
                                nextSampleAt += SAMPLE_INTERVAL_MS;
                            }
                            Thread.sleep(100);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        samplerStopped.countDown();
                    }
                },
                "soak-sampler");
        sampler.setDaemon(true);

        driver.start();
        sampler.start();

        Thread.sleep(SOAK_DURATION_MS);
        keepRunning.set(false);

        assertTrue(driverStopped.await(10, TimeUnit.SECONDS), "driver thread did not stop within 10 s");
        assertTrue(samplerStopped.await(10, TimeUnit.SECONDS), "sampler thread did not stop within 10 s");

        try {
            writeCsv(samples);

            long baseline = meanInWindow(samples, BASELINE_WINDOW_START_MS, BASELINE_WINDOW_END_MS);
            long peak = maxInWindow(samples, PEAK_WINDOW_START_MS, PEAK_WINDOW_END_MS);
            long growth = peak - baseline;
            System.out.printf(
                    "Soak result: baseline=%d bytes, peak=%d bytes, growth=%d bytes%n", baseline, peak, growth);
            assertTrue(
                    growth < MAX_GROWTH_BYTES,
                    "heap growth " + growth + " bytes exceeded threshold " + MAX_GROWTH_BYTES + " bytes");
        } finally {
            reporter.close();
        }
    }

    private static long meanInWindow(List<Sample> samples, long startMs, long endMs) {
        long sum = 0;
        long count = 0;
        for (Sample s : samples) {
            if (s.elapsedMs >= startMs && s.elapsedMs <= endMs) {
                sum += s.heapRuntimeBytes;
                count++;
            }
        }
        if (count == 0) throw new AssertionError("no samples in window [" + startMs + ", " + endMs + "] ms");
        return sum / count;
    }

    private static long maxInWindow(List<Sample> samples, long startMs, long endMs) {
        long max = Long.MIN_VALUE;
        boolean found = false;
        for (Sample s : samples) {
            if (s.elapsedMs >= startMs && s.elapsedMs <= endMs) {
                if (s.heapRuntimeBytes > max) max = s.heapRuntimeBytes;
                found = true;
            }
        }
        if (!found) throw new AssertionError("no samples in window [" + startMs + ", " + endMs + "] ms");
        return max;
    }

    private static void writeCsv(List<Sample> samples) throws IOException {
        Path out = Path.of("build/reports/soak/heap.csv");
        Files.createDirectories(out.getParent());
        StringBuilder sb = new StringBuilder("elapsed_ms,heap_runtime_bytes,heap_mxbean_bytes\n");
        for (Sample s : samples) {
            sb.append(s.elapsedMs)
                    .append(',')
                    .append(s.heapRuntimeBytes)
                    .append(',')
                    .append(s.heapMxbeanBytes)
                    .append('\n');
        }
        Files.writeString(out, sb.toString());
    }

    private record Sample(long elapsedMs, long heapRuntimeBytes, long heapMxbeanBytes) {}
}
