// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class AllowListTest {

    private static List<Pattern> patterns(String... regexes) {
        List<Pattern> ps = new java.util.ArrayList<>();
        for (String r : regexes) ps.add(Pattern.compile(r));
        return ps;
    }

    @Test
    void empty_list_allows_everything() {
        AllowList allowList = new AllowList(List.of());
        assertTrue(allowList.matches("anything.at.all"));
    }

    @Test
    void single_pattern_must_match_whole_subject() {
        AllowList allowList = new AllowList(patterns("producer-metrics\\..*"));
        assertTrue(allowList.matches("producer-metrics.record-send-rate"));
        assertFalse(allowList.matches("xproducer-metrics.record-send-rate"));
        assertFalse(allowList.matches("consumer-metrics.records-consumed-rate"));
    }

    @Test
    void any_of_several_patterns_admits_the_subject() {
        AllowList allowList = new AllowList(patterns("consumer.*", "producer.*"));
        assertTrue(allowList.matches("producer-metrics.record-send-rate"));
        assertTrue(allowList.matches("consumer-metrics.records-consumed-rate"));
        assertFalse(allowList.matches("kafka.server.BytesInPerSec"));
    }

    @Test
    void backreference_in_one_pattern_is_not_corrupted_by_another() {
        // A backreference refers to a group by number. Folding multiple patterns into one
        // alternation renumbers groups, so the second pattern's "\\1" would point at the
        // first pattern's group instead of its own. Evaluating patterns independently keeps
        // each backreference bound to its own capture group.
        AllowList allowList = new AllowList(patterns("(\\w+)\\.first", "(\\w+)\\.\\1"));
        // Second pattern means: "<word>.<same word>". "dup.dup" matches; "a.b" does not.
        assertTrue(allowList.matches("dup.dup"));
        assertFalse(allowList.matches("a.b"));
        // First pattern still works independently.
        assertTrue(allowList.matches("anything.first"));
    }
}
