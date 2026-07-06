// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter.clienttelemetry;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import dev.monedula.metricsreporter.clienttelemetry.ClientMetricsEnricher.ClientIdentity;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.telemetry.ClientTelemetryPayload;
import org.junit.jupiter.api.Test;

class ClientTelemetryReceiverImplTest {

    /** Test double capturing what the receiver hands to the forwarder. */
    static final class Sink implements ClientTelemetryReceiverImpl.Submitter {
        final AtomicReference<byte[]> payload = new AtomicReference<>();
        final AtomicReference<ClientIdentity> identity = new AtomicReference<>();

        @Override
        public boolean submit(byte[] p, ClientIdentity id) {
            payload.set(p);
            identity.set(id);
            return true;
        }
    }

    private static final Uuid FIXED_UUID = Uuid.randomUuid();

    private static ClientTelemetryPayload payloadOf(byte[] bytes) {
        ClientTelemetryPayload p = mock(ClientTelemetryPayload.class);
        when(p.data()).thenReturn(ByteBuffer.wrap(bytes));
        when(p.clientInstanceId()).thenReturn(FIXED_UUID);
        return p;
    }

    private static AuthorizableRequestContext ctx() throws Exception {
        AuthorizableRequestContext c = mock(AuthorizableRequestContext.class);
        when(c.principal()).thenReturn(new KafkaPrincipal("User", "alice"));
        when(c.clientAddress()).thenReturn(InetAddress.getByName("10.0.0.7"));
        when(c.clientId()).thenReturn("my-client");
        return c;
    }

    @Test
    void copies_payload_and_omits_principal_address_when_disabled() throws Exception {
        var sink = new Sink();
        var receiver = new ClientTelemetryReceiverImpl(sink, false);
        byte[] src = {1, 2, 3};
        ByteBuffer buf = ByteBuffer.wrap(src);
        ClientTelemetryPayload p = mock(ClientTelemetryPayload.class);
        when(p.data()).thenReturn(buf);
        when(p.clientInstanceId()).thenReturn(FIXED_UUID);

        receiver.exportMetrics(ctx(), p);

        assertArrayEquals(new byte[] {1, 2, 3}, sink.payload.get());
        // The copy must be independent of the source buffer's backing array.
        src[0] = 99;
        assertEquals(1, sink.payload.get()[0]);
        // Identity is always built now; principal/address are null when captureClientIdentity is false.
        assertNotNull(sink.identity.get());
        assertNull(sink.identity.get().principal());
        assertNull(sink.identity.get().address());
    }

    @Test
    void captures_identity_when_enabled() throws Exception {
        var sink = new Sink();
        var receiver = new ClientTelemetryReceiverImpl(sink, true);
        receiver.exportMetrics(ctx(), payloadOf(new byte[] {4}));

        assertEquals("User:alice", sink.identity.get().principal());
        assertEquals("10.0.0.7", sink.identity.get().address());
    }

    @Test
    void always_captures_client_id_and_instance_id_regardless_of_capture_flag() throws Exception {
        var sink = new Sink();
        var receiver = new ClientTelemetryReceiverImpl(sink, false);
        receiver.exportMetrics(ctx(), payloadOf(new byte[] {4}));

        assertNotNull(sink.identity.get());
        assertEquals("my-client", sink.identity.get().clientId());
        assertEquals(FIXED_UUID.toString(), sink.identity.get().clientInstanceId());
    }

    @Test
    void captures_client_id_and_instance_id_also_when_capture_flag_enabled() throws Exception {
        var sink = new Sink();
        var receiver = new ClientTelemetryReceiverImpl(sink, true);
        receiver.exportMetrics(ctx(), payloadOf(new byte[] {4}));

        assertNotNull(sink.identity.get());
        assertEquals("my-client", sink.identity.get().clientId());
        assertEquals(FIXED_UUID.toString(), sink.identity.get().clientInstanceId());
    }

    @Test
    void client_id_is_null_when_context_is_null() throws Exception {
        var sink = new Sink();
        var receiver = new ClientTelemetryReceiverImpl(sink, false);
        ClientTelemetryPayload p = payloadOf(new byte[] {4});
        receiver.exportMetrics(null, p);

        assertNotNull(sink.identity.get());
        assertNull(sink.identity.get().clientId());
    }

    @Test
    void tolerates_null_payload_data() throws Exception {
        var sink = new Sink();
        var receiver = new ClientTelemetryReceiverImpl(sink, false);
        ClientTelemetryPayload p = mock(ClientTelemetryPayload.class);
        when(p.data()).thenReturn(null);
        // Must not throw on a broker request-handler thread.
        receiver.exportMetrics(ctx(), p);
        assertNull(sink.payload.get());
    }

    @Test
    void tolerates_null_payload() {
        var sink = new Sink();
        var receiver = new ClientTelemetryReceiverImpl(sink, false);
        receiver.exportMetrics(null, null); // must not throw
        assertNull(sink.payload.get());
    }
}
