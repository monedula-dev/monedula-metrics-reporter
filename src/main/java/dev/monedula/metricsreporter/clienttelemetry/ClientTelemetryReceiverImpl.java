// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter.clienttelemetry;

import dev.monedula.metricsreporter.clienttelemetry.ClientMetricsEnricher.ClientIdentity;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.telemetry.ClientTelemetryPayload;
import org.apache.kafka.server.telemetry.ClientTelemetryReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KIP-714 receiver. Kafka invokes {@link #exportMetrics} on a broker request-handler thread
 * for each client push. This method must stay cheap: copy the payload bytes (the buffer is
 * not valid after return), optionally capture the client's identity, and hand off to the
 * forwarder's non-blocking submit. No parsing, no I/O here.
 */
public final class ClientTelemetryReceiverImpl implements ClientTelemetryReceiver {

    private static final Logger log = LoggerFactory.getLogger(ClientTelemetryReceiverImpl.class);

    /** Seam over {@link ClientTelemetryForwarder#submit} so the receiver is unit-testable in isolation. */
    public interface Submitter {
        boolean submit(byte[] payload, ClientIdentity clientIdentity);
    }

    private final Submitter submitter;
    private final boolean captureClientIdentity;

    public ClientTelemetryReceiverImpl(Submitter submitter, boolean captureClientIdentity) {
        this.submitter = submitter;
        this.captureClientIdentity = captureClientIdentity;
    }

    @Override
    public void exportMetrics(AuthorizableRequestContext context, ClientTelemetryPayload payload) {
        try {
            ByteBuffer data = payload == null ? null : payload.data();
            if (data == null || !data.hasRemaining()) {
                return;
            }
            byte[] copy = new byte[data.remaining()];
            data.duplicate().get(copy);

            String clientId = context == null ? null : context.clientId();
            org.apache.kafka.common.Uuid instanceId = payload.clientInstanceId();
            String clientInstanceId = instanceId == null ? null : instanceId.toString();

            String principal = null;
            String address = null;
            if (captureClientIdentity && context != null) {
                principal =
                        context.principal() == null ? "" : context.principal().toString();
                InetAddress addr = context.clientAddress();
                address = addr == null ? "" : addr.getHostAddress();
            }
            ClientIdentity identity = new ClientIdentity(principal, address, clientId, clientInstanceId);
            submitter.submit(copy, identity);
        } catch (Throwable t) {
            // Never let a telemetry hiccup disturb the broker request path.
            if (t instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.debug("Failed to accept client telemetry payload — ignoring", t);
        }
    }
}
