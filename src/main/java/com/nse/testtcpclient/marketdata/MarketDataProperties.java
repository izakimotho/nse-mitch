package com.nse.testtcpclient.marketdata;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

@ConfigurationProperties(prefix = "marketdata")
public record MarketDataProperties(
        @DefaultValue("true") boolean enabled,
        String host,
        @DefaultValue("13496") int port,
        String username,
        String password,
        @DefaultValue("5s") Duration connectTimeout,
        /* 0 = block indefinitely; set above the heartbeat interval to detect a silent peer. */
        @DefaultValue("0s") Duration readTimeout,
        @DefaultValue("10s") Duration loginTimeout,
        @DefaultValue("10s") Duration acceptTimeout,
        @DefaultValue("10s") Duration completionTimeout,
        @DefaultValue("3") int maxConnectAttempts,
        @DefaultValue("5s") Duration retryBackoff,
        /* Whether inbound inner-message lengths count their own 2-byte length field. */
        @DefaultValue("false") boolean innerLengthIncludesLengthField,
        @DefaultValue("REPLAY") SyncMode syncMode,
        @DefaultValue Replay replay,
        @DefaultValue Snapshot snapshot) {

    public enum SyncMode { REPLAY, SNAPSHOT, NONE }

    public record Replay(
            @DefaultValue("1") int startSequence,
            @DefaultValue("0") int count,
            @DefaultValue("1") byte marketDataGroup) {
    }

    public record Snapshot(
            @DefaultValue("1") int requestId,
            @DefaultValue("0") int instrumentId,
            @DefaultValue("1") byte marketDataGroup,
            @DefaultValue("1") byte snapshotType) {
    }

    public MarketDataProperties {
        if (enabled) {
            requireText(host, "marketdata.host");
            requireText(username, "marketdata.username");
            requireText(password, "marketdata.password");
        }
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException("marketdata.port must be between 1 and 65535");
        }
        if (maxConnectAttempts < 1) {
            throw new IllegalArgumentException("marketdata.max-connect-attempts must be at least 1");
        }
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required when marketdata.enabled=true");
        }
    }

    @Override
    public String toString() {
        return "MarketDataProperties[host=%s, port=%d, username=%s, password=****, syncMode=%s, replay=%s, snapshot=%s]"
                .formatted(host, port, username, syncMode, replay, snapshot);
    }
}
