// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Allow-list of compiled regex patterns applied at export time by the collector.
 * An empty pattern list means "allow all" — same semantics as the user-facing
 * {@code otlp.metric.reporter.allowed.metrics} configuration default.
 *
 * <p>{@link #matches(String)} tests each user pattern with {@code Matcher.matches()}
 * (whole-subject anchoring) and short-circuits on the first hit. Each pattern is kept
 * and evaluated independently rather than folded into a single alternation: combining
 * them renumbers each pattern's capture groups, which silently changes the meaning of
 * any backreference (e.g. {@code (\w+)-\1}). Iterating a handful of configured patterns
 * is cheap and, unlike the alternation, always correct.
 *
 * <p>Match results are memoized in a {@link ConcurrentHashMap} keyed by subject string.
 * The cache is bounded by the number of distinct subjects — metric names of the form
 * {@code {group}.{name}} (SPI) or {@code {group}.{type}.{name}} (Yammer), which contain
 * no tag values — so it cannot grow with cardinality. An {@code AllowList} instance is
 * immutable apart from this cache; a reconfigure swaps in a brand-new instance wholesale,
 * discarding the old memo along with the old patterns.
 */
public final class AllowList {

    /** Shared "allow everything" instance — the default filter before any config is applied. */
    public static final AllowList ALLOW_ALL = new AllowList(List.of());

    private final List<Pattern> patterns;
    private final ConcurrentHashMap<String, Boolean> memo = new ConcurrentHashMap<>();

    public AllowList(List<Pattern> patterns) {
        this.patterns = List.copyOf(patterns);
    }

    public boolean matches(String subject) {
        if (patterns.isEmpty()) return true;
        return memo.computeIfAbsent(subject, this::evaluate);
    }

    private boolean evaluate(String subject) {
        for (Pattern p : patterns) {
            if (p.matcher(subject).matches()) return true;
        }
        return false;
    }
}
