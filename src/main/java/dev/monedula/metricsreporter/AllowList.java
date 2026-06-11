// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Allow-list of compiled regex patterns shared by the SPI and Yammer registries.
 * An empty pattern list means "allow all" — same semantics as the user-facing
 * {@code otlp.metric.reporter.allowed.metrics} configuration default.
 *
 * <p>{@link #matches(String)} tests each user pattern with {@code Matcher.matches()}
 * (whole-subject anchoring) and short-circuits on the first hit. Each pattern is kept
 * and evaluated independently rather than folded into a single alternation: combining
 * them renumbers each pattern's capture groups, which silently changes the meaning of
 * any backreference (e.g. {@code (\w+)-\1}). This runs on the metric add/change
 * callback — a per-registration path, not a per-measurement one — so iterating a
 * handful of configured patterns is cheap and, unlike the alternation, always correct.
 */
public final class AllowList {

    private final List<Pattern> patterns;

    public AllowList(List<Pattern> patterns) {
        this.patterns = List.copyOf(patterns);
    }

    public boolean matches(String subject) {
        if (patterns.isEmpty()) return true;
        for (Pattern p : patterns) {
            if (p.matcher(subject).matches()) return true;
        }
        return false;
    }
}
