// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Allow-list of compiled regex patterns shared by the SPI and Yammer registries.
 * An empty pattern list means "allow all" — same semantics as the user-facing
 * {@code otlp.metric.reporter.allowed.metrics} configuration default.
 *
 * <p>Multiple patterns are combined into one anchored alternation at construction
 * so {@link #matches(String)} runs a single {@link java.util.regex.Matcher} pass
 * regardless of how many user patterns were configured — avoiding the per-call
 * {@code matcher()} allocation and N-deep iteration on Kafka's metric-callback
 * thread (a hot path called per metric add/change).
 */
public final class AllowList {

    private final Pattern combined;

    public AllowList(List<Pattern> patterns) {
        if (patterns.isEmpty()) {
            this.combined = null;
        } else if (patterns.size() == 1) {
            this.combined = patterns.get(0);
        } else {
            // Wrap each user pattern in a non-capturing group so its own anchors / flags
            // can't bleed into the next alternative. matches() anchors the whole thing,
            // preserving the per-pattern "must match entire subject" semantics.
            String joined =
                    patterns.stream().map(p -> "(?:" + p.pattern() + ")").collect(Collectors.joining("|"));
            this.combined = Pattern.compile(joined);
        }
    }

    public boolean matches(String subject) {
        if (combined == null) return true;
        return combined.matcher(subject).matches();
    }
}
