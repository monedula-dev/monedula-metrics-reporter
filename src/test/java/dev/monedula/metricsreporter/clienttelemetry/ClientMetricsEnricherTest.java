// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter.clienttelemetry;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import dev.monedula.metricsreporter.clienttelemetry.ClientMetricsEnricher.ClientIdentity;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ClientMetricsEnricherTest {

    private static io.opentelemetry.proto.resource.v1.Resource clientResource(String key, String value) {
        return io.opentelemetry.proto.resource.v1.Resource.newBuilder()
                .addAttributes(KeyValue.newBuilder()
                        .setKey(key)
                        .setValue(AnyValue.newBuilder().setStringValue(value).build())
                        .build())
                .build();
    }

    @Test
    void carries_client_attributes_when_both_toggles_off() {
        var enricher = new ClientMetricsEnricher(false, false, false, false);
        io.opentelemetry.sdk.resources.Resource r =
                enricher.enrich(clientResource("client_instance_id", "abc"), Map.of("kafka.cluster.id", "C1"), null);
        assertEquals("abc", r.getAttribute(stringKey("client_instance_id")));
        assertNull(r.getAttribute(stringKey("kafka.cluster.id")));
    }

    @Test
    void broker_wins_on_conflict_when_enrich_broker_enabled() {
        var enricher = new ClientMetricsEnricher(true, false, false, false);
        io.opentelemetry.sdk.resources.Resource r = enricher.enrich(
                clientResource("kafka.cluster.id", "SPOOFED"),
                Map.of("kafka.cluster.id", "REAL", "kafka.node.id", "1"),
                null);
        assertEquals("REAL", r.getAttribute(stringKey("kafka.cluster.id")));
        assertEquals("1", r.getAttribute(stringKey("kafka.node.id")));
    }

    @Test
    void adds_client_identity_only_when_toggle_on() {
        var off = new ClientMetricsEnricher(false, false, false, false);
        assertNull(
                off.enrich(clientResource("a", "b"), Map.of(), new ClientIdentity("User:alice", "10.0.0.1", null, null))
                        .getAttribute(stringKey("kafka.client.principal")));

        var on = new ClientMetricsEnricher(false, true, false, false);
        io.opentelemetry.sdk.resources.Resource r =
                on.enrich(clientResource("a", "b"), Map.of(), new ClientIdentity("User:alice", "10.0.0.1", null, null));
        assertEquals("User:alice", r.getAttribute(stringKey("kafka.client.principal")));
        assertEquals("10.0.0.1", r.getAttribute(stringKey("kafka.client.address")));
    }

    @Test
    void adds_client_id_only_when_enrich_client_id_enabled() {
        var off = new ClientMetricsEnricher(false, false, false, false);
        assertNull(off.enrich(clientResource("a", "b"), Map.of(), new ClientIdentity(null, null, "my-client", null))
                .getAttribute(stringKey("client_id")));

        var on = new ClientMetricsEnricher(false, false, true, false);
        io.opentelemetry.sdk.resources.Resource r =
                on.enrich(clientResource("a", "b"), Map.of(), new ClientIdentity(null, null, "my-client", null));
        assertEquals("my-client", r.getAttribute(stringKey("client_id")));
    }

    @Test
    void client_id_not_added_when_blank_or_null() {
        var enricher = new ClientMetricsEnricher(false, false, true, false);
        assertNull(enricher.enrich(clientResource("a", "b"), Map.of(), new ClientIdentity(null, null, "", null))
                .getAttribute(stringKey("client_id")));
        assertNull(enricher.enrich(clientResource("a", "b"), Map.of(), new ClientIdentity(null, null, null, null))
                .getAttribute(stringKey("client_id")));
    }

    @Test
    void adds_client_instance_id_only_when_enrich_client_instance_id_enabled() {
        var off = new ClientMetricsEnricher(false, false, false, false);
        assertNull(off.enrich(clientResource("a", "b"), Map.of(), new ClientIdentity(null, null, null, "inst-uuid-123"))
                .getAttribute(stringKey("client_instance_id")));

        var on = new ClientMetricsEnricher(false, false, false, true);
        io.opentelemetry.sdk.resources.Resource r =
                on.enrich(clientResource("a", "b"), Map.of(), new ClientIdentity(null, null, null, "inst-uuid-123"));
        assertEquals("inst-uuid-123", r.getAttribute(stringKey("client_instance_id")));
    }

    @Test
    void client_instance_id_not_added_when_blank_or_null() {
        var enricher = new ClientMetricsEnricher(false, false, false, true);
        assertNull(enricher.enrich(clientResource("a", "b"), Map.of(), new ClientIdentity(null, null, null, ""))
                .getAttribute(stringKey("client_instance_id")));
        assertNull(enricher.enrich(clientResource("a", "b"), Map.of(), new ClientIdentity(null, null, null, null))
                .getAttribute(stringKey("client_instance_id")));
    }

    @Test
    void stringify_converts_non_string_scalars() {
        assertEquals(
                "42",
                ClientMetricsEnricher.stringify(
                        AnyValue.newBuilder().setIntValue(42).build()));
        assertEquals(
                "true",
                ClientMetricsEnricher.stringify(
                        AnyValue.newBuilder().setBoolValue(true).build()));
        assertEquals("", ClientMetricsEnricher.stringify(AnyValue.getDefaultInstance())); // VALUE_NOT_SET
    }
}
