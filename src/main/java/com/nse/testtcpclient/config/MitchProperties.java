package com.nse.testtcpclient.config;

import lombok.Data;
import lombok.ToString;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Data
@Configuration
@ConfigurationProperties(prefix = "nse.mitch.replay")
public class MitchProperties implements InitializingBean {

    private static final String PREFIX = "nse.mitch.replay.";

    public enum SyncMode { REPLAY, SNAPSHOT, NONE }

    private boolean enabled = true;
    private String host;
    private int port = 13496;
    /** Used instead of {@code port} when sync-mode is SNAPSHOT. */
    private int snapshotPort = 13496;
    private String username;
    @ToString.Exclude
    private String password;
    /** Market data group sent in replay and snapshot requests. */
    private byte marketDataGroup = 1;
    /** Not used by the client (the server sends heartbeats); kept for existing callers. */
    private int heartbeatIntervalSeconds = 5;
    /** Socket read timeout; 0 blocks indefinitely. Keep it above the heartbeat interval. */
    private int socketTimeoutMs = 10_000;
    private Duration connectTimeout = Duration.ofSeconds(5);
    private Duration loginTimeout = Duration.ofSeconds(10);
    private Duration acceptTimeout = Duration.ofSeconds(10);
    private Duration completionTimeout = Duration.ofSeconds(10);
    private int maxConnectAttempts = 3;
    private Duration retryBackoff = Duration.ofSeconds(5);
    /** Whether inbound inner-message lengths count their own 2-byte length field. */
    private boolean innerLengthIncludesLengthField = false;
    private SyncMode syncMode = SyncMode.REPLAY;
    private Replay replay = new Replay();
    private Snapshot snapshot = new Snapshot();

    @Data
    public static class Replay {
        private int startSequence = 1;
        private int count = 0;
    }

    @Data
    public static class Snapshot {
        private int requestId = 1;
        private int instrumentId = 0;
        private byte snapshotType = 1;
    }

    /** Port for the configured sync mode. */
    public int connectPort() {
        return syncMode == SyncMode.SNAPSHOT ? snapshotPort : port;
    }

    @Override
    public void afterPropertiesSet() {
        if (!enabled) {
            return;
        }
        requireText(host, "host");
        requireText(username, "username");
        requireText(password, "password");
        requirePort(port, "port");
        requirePort(snapshotPort, "snapshot-port");
        if (maxConnectAttempts < 1) {
            throw new IllegalArgumentException(PREFIX + "max-connect-attempts must be at least 1");
        }
        if (socketTimeoutMs < 0) {
            throw new IllegalArgumentException(PREFIX + "socket-timeout-ms must not be negative");
        }
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(PREFIX + name + " is required when " + PREFIX + "enabled=true");
        }
    }

    private static void requirePort(int value, String name) {
        if (value < 1 || value > 65_535) {
            throw new IllegalArgumentException(PREFIX + name + " must be between 1 and 65535");
        }
    }
}
