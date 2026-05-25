// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class MetricNameFormatterTest {

    @Test
    void hyphens_become_underscores() {
        assertEquals(
                "producer_metrics_record_send_rate",
                MetricNameFormatter.format("producer-metrics", "record-send-rate"));
    }

    @Test
    void dots_become_underscores() {
        assertEquals(
                "kafka_server_brokertopicmetrics_bytesinpersec",
                MetricNameFormatter.format("kafka.server.BrokerTopicMetrics", "BytesInPerSec"));
    }

    @Test
    void everything_is_lowercased() {
        assertEquals(
                "kafkacontroller_offlinepartitionscount",
                MetricNameFormatter.format("KafkaController", "OfflinePartitionsCount"));
    }

    @Test
    void consecutive_separators_collapse() {
        assertEquals("a_b", MetricNameFormatter.format("a..--", "--b"));
    }

    @Test
    void leading_and_trailing_separators_stripped() {
        assertEquals("foo_bar", MetricNameFormatter.format("-foo-", "-bar-"));
    }
}
