// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter;

/**
 * Flat lowercase naming, Strimzi-style:
 * - {group}_{name}
 * - lowercased
 * - any character that is not [a-z0-9_] is replaced by '_'
 * - consecutive underscores collapsed to one
 *
 * Examples:
 *   group="producer-metrics", name="record-send-rate"
 *     -> "producer_metrics_record_send_rate"
 *   group="kafka.server", name="BytesInPerSec"
 *     -> "kafka_server_bytesinpersec"
 */
public final class MetricNameFormatter {

    private MetricNameFormatter() {}

    public static String format(String group, String name) {
        return sanitize(group + "_" + name);
    }

    public static String sanitize(String raw) {
        String lower = raw.toLowerCase(java.util.Locale.ROOT);
        StringBuilder sb = new StringBuilder(lower.length());
        boolean lastUnderscore = false;
        for (int i = 0; i < lower.length(); i++) {
            char c = lower.charAt(i);
            boolean ok = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9');
            if (ok) {
                sb.append(c);
                lastUnderscore = false;
            } else {
                if (!lastUnderscore) {
                    sb.append('_');
                    lastUnderscore = true;
                }
            }
        }
        // strip leading/trailing underscore
        int start = 0, end = sb.length();
        while (start < end && sb.charAt(start) == '_') start++;
        while (end > start && sb.charAt(end - 1) == '_') end--;
        return sb.substring(start, end);
    }
}
