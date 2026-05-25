# Benchmarks

Quantitative evidence behind three load-bearing claims in this project's
docs.

## What each suite measures

| Suite | File | Backs |
|---|---|---|
| JMH callback hot-path | [`MetricChangeBenchmark`](src/jmh/java/dev/monedula/metricsreporter/bench/MetricChangeBenchmark.java) | "callbacks are cheap, no I/O on the calling thread" |
| Heap soak vs. dead collector | [`BoundedMemorySoakTest`](src/test/java/dev/monedula/metricsreporter/bench/BoundedMemorySoakTest.java) | "no retry queue, bounded memory" |
| Slow-collector timeout behavior | [`ExportTimeoutBehaviorTest`](src/test/java/dev/monedula/metricsreporter/bench/ExportTimeoutBehaviorTest.java) | "slow collector can't stall Kafka — export times out, batch drops, next tick fresh" |

## Running locally

Requires JDK 17+, Docker (for the timeout test's Testcontainers stack), and
the project's Gradle wrapper.

```bash
# JMH microbenchmark only — ~2.5 minutes
./gradlew :benchmarks:jmh

# JUnit suite (soak + timeout) — ~6 minutes (soak alone is 5)
./gradlew :benchmarks:test

# Whole suite
./gradlew :benchmarks:jmh :benchmarks:test
```

The nightly CI workflow [`.github/workflows/benchmarks.yml`](../.github/workflows/benchmarks.yml)
runs the same commands and uploads the result tree as a workflow artifact
named `benchmark-reports`.

## Reading the results

### JMH (`build/reports/jmh/results.json` + `.txt`)

`results.json` has one entry per scenario (3 thread-counts × 3 allow-list
shapes = 9 entries) per mode (AverageTime + SampleTime). The fields that
matter:

- `primaryMetric.score` — ns/op for `Mode.AverageTime`. Sub-microsecond
  numbers across all scenarios are the expected baseline.
- `primaryMetric.scorePercentiles` — percentile distribution for
  `Mode.SampleTime`. Look at `p99.99` for tail-latency stability.

A `.txt` summary in the same directory is easier to skim at a glance.

### Soak (`build/reports/soak/heap.csv`)

CSV with one row per 5-second sample: `elapsed_ms,heap_runtime_bytes,heap_mxbean_bytes`.
The test's own assertion compares the peak in seconds 240–300 against the
mean in seconds 30–60 and fails if growth exceeds 100 MB. The CSV is for
plotting and trend inspection beyond that single number; load it into
any spreadsheet or `pandas.read_csv`.

### Timeout (`build/test-results/test/*.xml`)

Standard JUnit Jupiter XML. Pass means the slow collector didn't stall
the scheduler and no batch was delivered past its timeout. Failure
messages name the assertion that fired and include the relevant
container log excerpt.

## Out of scope

Trend tracking, hard CI thresholds, profiler integration, and
cross-product comparison are deliberately not in this suite. Cloud-runner
noise makes hard thresholds brittle; the precise numbers in artifacts are
for human review, with each test carrying a generous safety-net assertion
that fails only on a catastrophic regression.
