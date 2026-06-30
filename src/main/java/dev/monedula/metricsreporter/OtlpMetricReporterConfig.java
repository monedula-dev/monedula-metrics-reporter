// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.kafka.common.config.ConfigException;

public class OtlpMetricReporterConfig {

    // Public so operators reading the source (e.g., chasing a logged ConfigException)
    // can find every configurable key on this class without spelunking the parsing
    // helpers. These are the canonical names of the keys Kafka passes via
    // AbstractConfig#originalsWithPrefix.

    /**
     * OTLP collector endpoint URL. Default follows the transport:
     * {@code http://localhost:4317} for gRPC, {@code http://localhost:4318} for HTTP.
     */
    public static final String ENDPOINT = "otlp.metric.reporter.endpoint";
    /** OTLP transport. {@link #TRANSPORT_GRPC} or {@link #TRANSPORT_HTTP}. Default: {@code grpc}. */
    public static final String TRANSPORT = "otlp.metric.reporter.transport";
    /** Export interval in milliseconds. Default: {@code 30000}. */
    public static final String INTERVAL_MS = "otlp.metric.reporter.interval.ms";
    /** Per-export call timeout in milliseconds. Must be strictly less than {@link #INTERVAL_MS}. Default: {@code 5000}. */
    public static final String TIMEOUT_MS = "otlp.metric.reporter.timeout.ms";
    /** Comma-separated regex patterns used for allow-listing metrics. Empty = allow all. */
    public static final String ALLOWED_METRICS = "otlp.metric.reporter.allowed.metrics";
    /** Extra OTLP resource attributes as {@code key=value,key=value}. */
    public static final String RESOURCE_ATTRS = "otlp.metric.reporter.resource.attributes";
    /** Static OTLP request headers as {@code Header-Name=value,Other=value}. Use for collector auth. */
    public static final String HEADERS = "otlp.metric.reporter.headers";
    /** Exporter compression. {@link #COMPRESSION_NONE} or {@link #COMPRESSION_GZIP}. Default: {@code none}. */
    public static final String COMPRESSION = "otlp.metric.reporter.compression";
    /** Optional PEM CA bundle path for trusting the collector TLS cert. */
    public static final String TRUSTED_CERTIFICATES_PATH = "otlp.metric.reporter.trusted.certificates.path";
    /** Optional PEM client cert path for mTLS. Must be configured together with {@link #CLIENT_KEY_PATH}. */
    public static final String CLIENT_CERTIFICATE_PATH = "otlp.metric.reporter.client.certificate.path";
    /** Optional PEM client key path for mTLS. Must be configured together with {@link #CLIENT_CERTIFICATE_PATH}. */
    public static final String CLIENT_KEY_PATH = "otlp.metric.reporter.client.key.path";
    /** Whether to also export JVM runtime metrics. Default: {@code true}. */
    public static final String JVM_METRICS_ENABLED = "otlp.metric.reporter.jvm.metrics.enabled";
    /** Kill-switch for KIP-714 client telemetry. When false, clientReceiver() returns null. Default: {@code true}. */
    public static final String CLIENT_TELEMETRY_ENABLED = "otlp.metric.reporter.client.telemetry.enabled";
    /** Add broker identity (cluster/node) to forwarded client metrics. Default: {@code true}. */
    public static final String CLIENT_TELEMETRY_ENRICH_BROKER = "otlp.metric.reporter.client.telemetry.enrich.broker";
    /** Add authenticated principal + client address to forwarded client metrics (PII/cardinality). Default: {@code false}. */
    public static final String CLIENT_TELEMETRY_ENRICH_CLIENT_IDENTITY =
            "otlp.metric.reporter.client.telemetry.enrich.client.identity";
    /** Add client_id to forwarded client metrics as a resource attribute. Default: {@code true}. */
    public static final String CLIENT_TELEMETRY_ENRICH_CLIENT_ID =
            "otlp.metric.reporter.client.telemetry.enrich.client.id";
    /** Add client_instance_id to forwarded client metrics as a resource attribute (opt-in). Default: {@code false}. */
    public static final String CLIENT_TELEMETRY_ENRICH_CLIENT_INSTANCE_ID =
            "otlp.metric.reporter.client.telemetry.enrich.client.instance.id";
    /** Bounded in-memory queue size for inbound client pushes; overflow drops. Default: {@code 1024}. */
    public static final String CLIENT_TELEMETRY_QUEUE_CAPACITY = "otlp.metric.reporter.client.telemetry.queue.capacity";

    public static final String TRANSPORT_GRPC = "grpc";
    public static final String TRANSPORT_HTTP = "http";

    /** Default collector endpoint for the gRPC transport (OTLP/gRPC standard port). */
    public static final String DEFAULT_GRPC_ENDPOINT = "http://localhost:4317";
    /** Default collector endpoint for the HTTP transport (OTLP/HTTP standard port). */
    public static final String DEFAULT_HTTP_ENDPOINT = "http://localhost:4318";

    public static final String COMPRESSION_NONE = "none";
    public static final String COMPRESSION_GZIP = "gzip";
    private static final Set<String> SUPPORTED_TRANSPORTS = Set.of(TRANSPORT_GRPC, TRANSPORT_HTTP);
    private static final Set<String> SUPPORTED_COMPRESSION = Set.of(COMPRESSION_NONE, COMPRESSION_GZIP);

    private static final Map<String, String> DEFAULTS = Map.ofEntries(
            Map.entry(ENDPOINT, DEFAULT_GRPC_ENDPOINT),
            Map.entry(TRANSPORT, TRANSPORT_GRPC),
            Map.entry(INTERVAL_MS, "30000"),
            Map.entry(TIMEOUT_MS, "5000"),
            Map.entry(ALLOWED_METRICS, ""),
            Map.entry(RESOURCE_ATTRS, ""),
            Map.entry(HEADERS, ""),
            Map.entry(COMPRESSION, COMPRESSION_NONE),
            Map.entry(TRUSTED_CERTIFICATES_PATH, ""),
            Map.entry(CLIENT_CERTIFICATE_PATH, ""),
            Map.entry(CLIENT_KEY_PATH, ""),
            Map.entry(JVM_METRICS_ENABLED, "true"),
            Map.entry(CLIENT_TELEMETRY_ENABLED, "true"),
            Map.entry(CLIENT_TELEMETRY_ENRICH_BROKER, "true"),
            Map.entry(CLIENT_TELEMETRY_ENRICH_CLIENT_IDENTITY, "false"),
            Map.entry(CLIENT_TELEMETRY_ENRICH_CLIENT_ID, "true"),
            Map.entry(CLIENT_TELEMETRY_ENRICH_CLIENT_INSTANCE_ID, "false"),
            Map.entry(CLIENT_TELEMETRY_QUEUE_CAPACITY, "1024"));

    /**
     * Canonical map of every configuration key this class understands to its default
     * value (or the empty string for optional keys with no default). The map is
     * immutable; modifying it throws {@link UnsupportedOperationException}.
     *
     * <p>Useful for tooling that needs to enumerate the reporter's configuration
     * surface programmatically — e.g., generating documentation, validating a
     * properties file before broker startup, or constructing a baseline config
     * that an operator then overrides.
     */
    public static Map<String, String> defaults() {
        return DEFAULTS;
    }

    private final String endpoint;
    private final String transport;
    private final long intervalMs;
    private final long timeoutMs;
    private final List<Pattern> allowedMetrics;
    private final Map<String, String> resourceAttributes;
    private final Map<String, String> headers;
    private final String compression;
    private final String trustedCertificatesPath;
    private final String clientCertificatePath;
    private final String clientKeyPath;
    private final boolean jvmMetricsEnabled;
    private final boolean clientTelemetryEnabled;
    private final boolean clientTelemetryEnrichBroker;
    private final boolean clientTelemetryEnrichClientIdentity;
    private final boolean clientTelemetryEnrichClientId;
    private final boolean clientTelemetryEnrichClientInstanceId;
    private final int clientTelemetryQueueCapacity;

    public OtlpMetricReporterConfig(Map<String, ?> configs) {
        this.transport = getString(configs, TRANSPORT, TRANSPORT_GRPC);
        // Default endpoint follows the chosen transport: OTLP/gRPC is :4317, OTLP/HTTP is :4318.
        // Using the gRPC port for an http transport (the old single default) silently produced
        // an unreachable endpoint whenever http was selected without an explicit endpoint.
        String defaultEndpoint = TRANSPORT_HTTP.equals(this.transport) ? DEFAULT_HTTP_ENDPOINT : DEFAULT_GRPC_ENDPOINT;
        URI parsedEndpoint = parseEndpoint(getString(configs, ENDPOINT, defaultEndpoint));
        this.endpoint = normalizeEndpoint(parsedEndpoint, this.transport);
        this.intervalMs = getLong(configs, INTERVAL_MS, 30_000L);
        this.timeoutMs = getLong(configs, TIMEOUT_MS, 5_000L);
        this.allowedMetrics = compilePatterns(configs);
        this.resourceAttributes = parseAttributes(getString(configs, RESOURCE_ATTRS, ""));
        this.headers = parseHeaders(getString(configs, HEADERS, ""));
        this.compression = parseCompression(getString(configs, COMPRESSION, COMPRESSION_NONE));
        this.trustedCertificatesPath = getOptionalString(configs, TRUSTED_CERTIFICATES_PATH);
        this.clientCertificatePath = getOptionalString(configs, CLIENT_CERTIFICATE_PATH);
        this.clientKeyPath = getOptionalString(configs, CLIENT_KEY_PATH);
        this.jvmMetricsEnabled = getBoolean(configs, JVM_METRICS_ENABLED, true);
        this.clientTelemetryEnabled = getBoolean(configs, CLIENT_TELEMETRY_ENABLED, true);
        this.clientTelemetryEnrichBroker = getBoolean(configs, CLIENT_TELEMETRY_ENRICH_BROKER, true);
        this.clientTelemetryEnrichClientIdentity = getBoolean(configs, CLIENT_TELEMETRY_ENRICH_CLIENT_IDENTITY, false);
        this.clientTelemetryEnrichClientId = getBoolean(configs, CLIENT_TELEMETRY_ENRICH_CLIENT_ID, true);
        this.clientTelemetryEnrichClientInstanceId =
                getBoolean(configs, CLIENT_TELEMETRY_ENRICH_CLIENT_INSTANCE_ID, false);
        // Safe narrowing: validate() rejects non-positive values, and realistic queue
        // capacities are far below Integer.MAX_VALUE, so the long->int cast cannot overflow in practice.
        this.clientTelemetryQueueCapacity = (int) getLong(configs, CLIENT_TELEMETRY_QUEUE_CAPACITY, 1024L);
        validate(parsedEndpoint);
    }

    private void validate(URI parsedEndpoint) {
        if (parsedEndpoint.getHost() == null) {
            throw new ConfigException(ENDPOINT, parsedEndpoint.toString(), "must be a valid URI: missing host");
        }
        if (!SUPPORTED_TRANSPORTS.contains(transport)) {
            throw new ConfigException(
                    TRANSPORT, transport, "must be '" + TRANSPORT_GRPC + "' or '" + TRANSPORT_HTTP + "'");
        }
        if (intervalMs <= 0) {
            throw new ConfigException(INTERVAL_MS, intervalMs, "must be > 0");
        }
        if (timeoutMs <= 0) {
            throw new ConfigException(TIMEOUT_MS, timeoutMs, "must be > 0");
        }
        // A timeout >= the export interval can let a slow export run right up to (or past)
        // the next scheduled tick and queue up behind it on the single-thread scheduler.
        // Require strict headroom so the scheduler always has a window to start the next tick.
        if (timeoutMs >= intervalMs) {
            throw new ConfigException(TIMEOUT_MS, timeoutMs, "must be < " + INTERVAL_MS + " (" + intervalMs + ")");
        }
        if (clientTelemetryQueueCapacity <= 0) {
            throw new ConfigException(CLIENT_TELEMETRY_QUEUE_CAPACITY, clientTelemetryQueueCapacity, "must be > 0");
        }
        if ((clientCertificatePath == null) != (clientKeyPath == null)) {
            throw new ConfigException(
                    CLIENT_CERTIFICATE_PATH + "/" + CLIENT_KEY_PATH,
                    clientCertificatePath != null ? clientCertificatePath : clientKeyPath,
                    "client certificate and client key paths must be configured together");
        }
    }

    private static URI parseEndpoint(String raw) {
        try {
            return new URI(raw);
        } catch (URISyntaxException e) {
            throw new ConfigException(ENDPOINT, raw, "must be a valid URI: " + e.getMessage());
        }
    }

    private static String normalizeEndpoint(URI parsed, String transport) {
        if (!TRANSPORT_HTTP.equals(transport)) return parsed.toString();
        String path = parsed.getPath();
        boolean bare = path == null || path.isBlank() || path.chars().allMatch(c -> c == '/');
        if (!bare) return parsed.toString(); // caller supplied an explicit signal path; leave it.
        // Append the OTLP/HTTP metrics path by rebuilding from components, so any query string
        // or fragment is preserved in place rather than being clobbered by string concatenation
        // (e.g. "http://host:4318?k=v" must become "http://host:4318/v1/metrics?k=v").
        try {
            return new URI(
                            parsed.getScheme(),
                            parsed.getAuthority(),
                            "/v1/metrics",
                            parsed.getQuery(),
                            parsed.getFragment())
                    .toString();
        } catch (URISyntaxException e) {
            throw new ConfigException(ENDPOINT, parsed.toString(), "could not normalize endpoint: " + e.getMessage());
        }
    }

    private static List<Pattern> compilePatterns(Map<String, ?> cfg) {
        Object v = cfg.get(ALLOWED_METRICS);
        if (v == null || v.toString().isBlank()) return Collections.emptyList();
        String[] raw = v.toString().split(",");
        List<Pattern> patterns = new ArrayList<>(raw.length);
        for (String s : raw) {
            try {
                patterns.add(Pattern.compile(s.trim()));
            } catch (PatternSyntaxException e) {
                throw new ConfigException(ALLOWED_METRICS, s.trim(), "invalid regex pattern: " + e.getMessage());
            }
        }
        return Collections.unmodifiableList(patterns);
    }

    private static String getString(Map<String, ?> cfg, String key, String def) {
        Object v = cfg.get(key);
        return v != null ? v.toString().trim() : def;
    }

    private static String getOptionalString(Map<String, ?> cfg, String key) {
        Object v = cfg.get(key);
        if (v == null || v.toString().trim().isEmpty()) return null;
        return v.toString().trim();
    }

    private static long getLong(Map<String, ?> cfg, String key, long def) {
        Object v = cfg.get(key);
        if (v == null) return def;
        try {
            return Long.parseLong(v.toString().trim());
        } catch (NumberFormatException e) {
            throw new ConfigException(key, v, "must be a long");
        }
    }

    private static boolean getBoolean(Map<String, ?> cfg, String key, boolean def) {
        Object v = cfg.get(key);
        if (v == null) return def;
        String s = v.toString().trim();
        // Boolean.parseBoolean treats anything except "true" as false, so a typo
        // ("tru", "yse") silently flips the flag. Be strict and signal the error.
        if (s.equalsIgnoreCase("true")) return true;
        if (s.equalsIgnoreCase("false")) return false;
        throw new ConfigException(key, v, "must be 'true' or 'false'");
    }

    private static Map<String, String> parseAttributes(String raw) {
        if (raw.isBlank()) return Collections.emptyMap();
        Map<String, String> result = new LinkedHashMap<>();
        for (String pair : raw.split(",")) {
            String[] kv = pair.split("=", 2);
            if (kv.length != 2 || kv[0].isBlank() || kv[1].isBlank()) {
                throw new ConfigException(RESOURCE_ATTRS, raw, "must be 'key=value,...'");
            }
            result.put(kv[0].trim(), kv[1].trim());
        }
        return Collections.unmodifiableMap(result);
    }

    private static Map<String, String> parseHeaders(String raw) {
        if (raw.isBlank()) return Collections.emptyMap();
        Map<String, String> result = new LinkedHashMap<>();
        for (String pair : raw.split(",", -1)) {
            String[] kv = pair.split("=", 2);
            if (kv.length != 2 || kv[0].isBlank() || kv[1].isBlank()) {
                throw new ConfigException(HEADERS, raw, "must be 'Header-Name=value,...'");
            }
            result.put(kv[0].trim(), kv[1].trim());
        }
        return Collections.unmodifiableMap(result);
    }

    private static String parseCompression(String raw) {
        String normalized = raw.trim().toLowerCase(Locale.ROOT);
        if (!SUPPORTED_COMPRESSION.contains(normalized)) {
            throw new ConfigException(
                    COMPRESSION, raw, "must be '" + COMPRESSION_NONE + "' or '" + COMPRESSION_GZIP + "'");
        }
        return normalized;
    }

    public String endpoint() {
        return endpoint;
    }

    public String transport() {
        return transport;
    }

    public long intervalMs() {
        return intervalMs;
    }

    public long timeoutMs() {
        return timeoutMs;
    }

    public List<Pattern> allowedMetrics() {
        return allowedMetrics;
    }

    public Map<String, String> resourceAttributes() {
        return resourceAttributes;
    }

    public Map<String, String> headers() {
        return headers;
    }

    public String compression() {
        return compression;
    }

    public Optional<String> trustedCertificatesPath() {
        return Optional.ofNullable(trustedCertificatesPath);
    }

    public Optional<String> clientCertificatePath() {
        return Optional.ofNullable(clientCertificatePath);
    }

    public Optional<String> clientKeyPath() {
        return Optional.ofNullable(clientKeyPath);
    }

    public boolean jvmMetricsEnabled() {
        return jvmMetricsEnabled;
    }

    public boolean clientTelemetryEnabled() {
        return clientTelemetryEnabled;
    }

    public boolean clientTelemetryEnrichBroker() {
        return clientTelemetryEnrichBroker;
    }

    public boolean clientTelemetryEnrichClientIdentity() {
        return clientTelemetryEnrichClientIdentity;
    }

    public boolean clientTelemetryEnrichClientId() {
        return clientTelemetryEnrichClientId;
    }

    public boolean clientTelemetryEnrichClientInstanceId() {
        return clientTelemetryEnrichClientInstanceId;
    }

    public int clientTelemetryQueueCapacity() {
        return clientTelemetryQueueCapacity;
    }
}
