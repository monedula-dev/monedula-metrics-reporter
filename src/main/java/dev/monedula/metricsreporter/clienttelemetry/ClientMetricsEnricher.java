// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter.clienttelemetry;

import static io.opentelemetry.api.common.AttributeKey.stringKey;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.sdk.resources.Resource;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Builds the OTLP {@link Resource} attached to forwarded KIP-714 client metrics.
 *
 * <p>Merge order (later wins): the client's own payload resource attributes, then broker
 * identity (so a client cannot spoof {@code kafka.cluster.id}), then optional client
 * identity. Each enrichment layer is gated by a config toggle.
 */
public final class ClientMetricsEnricher {

    /** Authenticated principal and source address of the pushing client (from the request context). */
    public record ClientIdentity(String principal, String address, String clientId, String clientInstanceId) {}

    private final boolean enrichBroker;
    private final boolean enrichClientIdentity;
    private final boolean enrichClientId;
    private final boolean enrichClientInstanceId;

    public ClientMetricsEnricher(
            boolean enrichBroker,
            boolean enrichClientIdentity,
            boolean enrichClientId,
            boolean enrichClientInstanceId) {
        this.enrichBroker = enrichBroker;
        this.enrichClientIdentity = enrichClientIdentity;
        this.enrichClientId = enrichClientId;
        this.enrichClientInstanceId = enrichClientInstanceId;
    }

    public Resource enrich(
            io.opentelemetry.proto.resource.v1.Resource clientResource,
            Map<String, String> brokerIdentity,
            ClientIdentity clientIdentity) {
        Map<String, String> merged = new LinkedHashMap<>();
        for (KeyValue kv : clientResource.getAttributesList()) {
            merged.put(kv.getKey(), stringify(kv.getValue()));
        }
        if (enrichBroker) {
            merged.putAll(brokerIdentity);
        }
        if (enrichClientIdentity && clientIdentity != null) {
            merged.put("kafka.client.principal", clientIdentity.principal());
            merged.put("kafka.client.address", clientIdentity.address());
        }
        if (enrichClientId && clientIdentity != null) {
            String clientId = clientIdentity.clientId();
            if (clientId != null && !clientId.isBlank()) {
                merged.put("client_id", clientId);
            }
        }
        if (enrichClientInstanceId && clientIdentity != null) {
            String clientInstanceId = clientIdentity.clientInstanceId();
            if (clientInstanceId != null && !clientInstanceId.isBlank()) {
                merged.put("client_instance_id", clientInstanceId);
            }
        }
        AttributesBuilder b = Attributes.builder();
        merged.forEach((k, v) -> b.put(stringKey(k), v));
        return Resource.create(b.build());
    }

    /**
     * Render an OTLP AnyValue as a string label. Scalar types are stringified; composite types
     * (array, kvlist, bytes) and unset values produce an empty string — safe as a metric label,
     * though the key then appears with no value.
     */
    static String stringify(AnyValue v) {
        return switch (v.getValueCase()) {
            case STRING_VALUE -> v.getStringValue();
            case BOOL_VALUE -> Boolean.toString(v.getBoolValue());
            case INT_VALUE -> Long.toString(v.getIntValue());
            case DOUBLE_VALUE -> Double.toString(v.getDoubleValue());
            default -> "";
        };
    }
}
