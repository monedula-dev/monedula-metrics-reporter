// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (C) 2025 Monedula contributors
package dev.monedula.metricsreporter;

import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.kafka.common.config.ConfigException;

public class OtlpExporterFactory {

    public static MetricExporter create(OtlpMetricReporterConfig config) {
        Duration timeout = Duration.ofMillis(config.timeoutMs());
        if (OtlpMetricReporterConfig.TRANSPORT_HTTP.equals(config.transport())) {
            var builder = OtlpHttpMetricExporter.builder()
                    .setEndpoint(config.endpoint())
                    .setTimeout(timeout);
            applyOptions(
                    config,
                    builder::addHeader,
                    builder::setCompression,
                    builder::setTrustedCertificates,
                    builder::setClientTls);
            return builder.build();
        }
        var builder =
                OtlpGrpcMetricExporter.builder().setEndpoint(config.endpoint()).setTimeout(timeout);
        applyOptions(
                config,
                builder::addHeader,
                builder::setCompression,
                builder::setTrustedCertificates,
                builder::setClientTls);
        return builder.build();
    }

    /**
     * Apply the shared collector-side options to whichever exporter builder is
     * being configured. The two OTel builder classes (HTTP / gRPC) don't share a
     * common interface, but they expose the same setter signatures — so we bind
     * each one through method references rather than duplicate this body twice.
     */
    private static void applyOptions(
            OtlpMetricReporterConfig config,
            BiConsumer<String, String> addHeader,
            Consumer<String> setCompression,
            Consumer<byte[]> setTrustedCertificates,
            BiConsumer<byte[], byte[]> setClientTls) {
        config.headers().forEach(addHeader);
        if (!OtlpMetricReporterConfig.COMPRESSION_NONE.equals(config.compression())) {
            setCompression.accept(config.compression());
        }
        config.trustedCertificatesPath()
                .ifPresent(path -> setTrustedCertificates.accept(
                        readFile(OtlpMetricReporterConfig.TRUSTED_CERTIFICATES_PATH, path)));
        if (config.clientCertificatePath().isPresent()) {
            byte[] cert = readFile(
                    OtlpMetricReporterConfig.CLIENT_CERTIFICATE_PATH,
                    config.clientCertificatePath().get());
            byte[] key = readFile(
                    OtlpMetricReporterConfig.CLIENT_KEY_PATH,
                    config.clientKeyPath().get());
            setClientTls.accept(cert, key);
        }
    }

    private static byte[] readFile(String configKey, String rawPath) {
        try {
            return Files.readAllBytes(Path.of(rawPath));
        } catch (IOException e) {
            throw new ConfigException(configKey, rawPath, "could not read file: " + e.getMessage());
        }
    }

    private OtlpExporterFactory() {}
}
