package com.nse.testtcpclient.marketdata;

import com.nse.testtcpclient.config.MitchProperties;
import com.nse.testtcpclient.marketdata.MarketDataRequestResult.Status;
import com.nse.testtcpclient.marketdata.protocol.FrameReader;
import com.nse.testtcpclient.marketdata.protocol.MitchDecoder;
import com.nse.testtcpclient.marketdata.protocol.MitchEncoder;
import com.nse.testtcpclient.marketdata.protocol.MitchMessage;
import com.nse.testtcpclient.marketdata.protocol.MitchMessage.AddOrder;
import com.nse.testtcpclient.marketdata.protocol.MitchMessage.HistoricalSymbol;
import com.nse.testtcpclient.marketdata.protocol.MitchMessage.InstrumentDefinition;
import com.nse.testtcpclient.marketdata.protocol.MitchMessage.LoginResponse;
import com.nse.testtcpclient.marketdata.protocol.MitchMessage.Malformed;
import com.nse.testtcpclient.marketdata.protocol.MitchMessage.ReplayResponse;
import com.nse.testtcpclient.marketdata.protocol.MitchMessage.SnapshotComplete;
import com.nse.testtcpclient.marketdata.protocol.MitchMessage.SymbolDirectory;
import com.nse.testtcpclient.marketdata.protocol.MitchMessage.SystemEvent;
import com.nse.testtcpclient.marketdata.request.ReplayRequestDto;
import com.nse.testtcpclient.marketdata.request.SnapshotRequestDto;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Runs replay and snapshot requests against the MITCH gateway. Each request gets its own connection: connect, log in,
 * send the request, consume until the completion marker, disconnect. Raw frames received after the request are
 * published as {@link MarketDataFrameEvent}s, decoded messages as {@link MarketDataMessageEvent}s and the outcome as a
 * {@link MarketDataRequestResult}.
 */
@Component
public class MarketDataClient {

    private static final Logger log = LoggerFactory.getLogger(MarketDataClient.class);
    private static final HexFormat HEX = HexFormat.of();

    private final MitchProperties properties;
    private final ApplicationEventPublisher publisher;
    private final MitchDecoder decoder;
    private final Map<Long, String> instrumentCache = new ConcurrentHashMap<>();
    private final Set<Session> sessions = ConcurrentHashMap.newKeySet();
    private volatile boolean stopped;

    public MarketDataClient(MitchProperties properties, ApplicationEventPublisher publisher) {
        this.properties = properties;
        this.publisher = publisher;
        this.decoder = new MitchDecoder(properties.isInnerLengthIncludesLengthField());
    }

    /** Runs a replay on the replay port and blocks until it completes, goes idle or fails. */
    public MarketDataRequestResult replay(ReplayRequestDto request) throws IOException, InterruptedException {
        int requestId = required(request.requestID(), "requestID");
        int startSequence = required(request.startSequence(), "startSequence");
        int count = required(request.count(), "count");
        byte group = groupOrDefault(request.marketDataGroup());
        return execute(RequestType.REPLAY, requestId, properties.getPort(), session -> {
            log.info("Replay request {} | start seq: {}, count: {}, group: {}", requestId, startSequence, count, group);
            session.send(MitchEncoder.replayRequest(startSequence, count, group), false);
            session.awaitReplayAccepted();
        });
    }

    /** Runs a snapshot on the snapshot port and blocks until it completes, goes idle or fails. */
    public MarketDataRequestResult snapshot(SnapshotRequestDto request) throws IOException, InterruptedException {
        int requestId = required(request.requestID(), "requestID");
        int instrumentId = required(request.instrumentId(), "instrumentId");
        byte snapshotType = required(request.snapshotType(), "snapshotType");
        byte group = groupOrDefault(request.marketDataGroup());
        return execute(RequestType.SNAPSHOT, requestId, properties.getSnapshotPort(), session -> {
            log.info("Snapshot request {} | instrument: {}, group: {}, type: {}",
                    requestId, instrumentId, group, snapshotType);
            session.send(MitchEncoder.snapshotRequest(requestId, instrumentId, group, snapshotType), false);
        });
    }

    /** Aborts in-flight requests and rejects new ones. */
    @PreDestroy
    public void stop() {
        stopped = true;
        sessions.forEach(Session::close);
    }

    public Optional<String> symbolFor(long instrumentId) {
        return Optional.ofNullable(instrumentCache.get(instrumentId));
    }

    private MarketDataRequestResult execute(RequestType type, int requestId, int port, RequestSender sender)
            throws IOException, InterruptedException {
        int maxAttempts = properties.getMaxConnectAttempts();
        for (int attempt = 1; ; attempt++) {
            Session session = new Session(type, requestId, port);
            try (session) {
                MarketDataRequestResult result = session.run(sender);
                publish(result);
                return result;
            } catch (IOException e) {
                // Retrying after data arrived would publish duplicates.
                boolean retry = !stopped && !(e instanceof MarketDataRejectedException)
                        && session.records() == 0 && attempt < maxAttempts;
                if (!retry) {
                    IOException failure = stopped
                            ? new MarketDataException("%s request %d aborted: client stopped".formatted(type, requestId), e)
                            : e;
                    publish(new MarketDataRequestResult(type, requestId, Status.FAILED, session.records(),
                            failure.getMessage()));
                    throw failure;
                }
                log.warn("{} request {} attempt {}/{} failed: {}. Retrying in {}",
                        type, requestId, attempt, maxAttempts, e.toString(), properties.getRetryBackoff());
            }
            Thread.sleep(properties.getRetryBackoff());
        }
    }

    private byte groupOrDefault(Byte group) {
        return group != null ? group : properties.getMarketDataGroup();
    }

    private static <T> T required(T value, String field) {
        if (value == null) {
            throw new IllegalArgumentException(field + " is required");
        }
        return value;
    }

    private void publish(Object event) {
        try {
            publisher.publishEvent(event);
        } catch (RuntimeException e) {
            log.error("Market data event listener failed for {}", event, e);
        }
    }

    private static <T> T await(CompletableFuture<T> future, Duration timeout, String what)
            throws IOException, InterruptedException {
        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new MarketDataException("Timed out after " + timeout + " waiting for " + what);
        } catch (ExecutionException e) {
            throw unwrap(e, what);
        }
    }

    private static IOException unwrap(ExecutionException e, String what) {
        return e.getCause() instanceof IOException io ? io : new MarketDataException("Failed waiting for " + what, e.getCause());
    }

    @FunctionalInterface
    private interface RequestSender {
        void send(Session session) throws IOException, InterruptedException;
    }

    /** One TCP connection for one request attempt. */
    private final class Session implements AutoCloseable {

        private final RequestType type;
        private final int requestId;
        private final int port;
        private final Socket socket = new Socket();
        private final ReentrantLock sendLock = new ReentrantLock();
        private final CompletableFuture<LoginResponse> loginResponse = new CompletableFuture<>();
        private final CompletableFuture<Void> replayAccepted = new CompletableFuture<>();
        private final CompletableFuture<Void> streamCompleted = new CompletableFuture<>();
        private final AtomicLong records = new AtomicLong();
        private volatile long lastActivityNanos = System.nanoTime();
        private volatile boolean closing;
        private volatile boolean requestSent;
        private OutputStream out;

        Session(RequestType type, int requestId, int port) {
            this.type = type;
            this.requestId = requestId;
            this.port = port;
            sessions.add(this);
            if (stopped) {
                close();
            }
        }

        MarketDataRequestResult run(RequestSender sender) throws IOException, InterruptedException {
            connect();
            login();
            requestSent = true;
            sender.send(this);
            Status status = awaitCompletion() ? Status.COMPLETED : Status.INCOMPLETE;
            return new MarketDataRequestResult(type, requestId, status, records.get(), null);
        }

        long records() {
            return records.get();
        }

        void awaitReplayAccepted() throws IOException, InterruptedException {
            await(replayAccepted, properties.getAcceptTimeout(), "replay acceptance");
        }

        void send(byte[] data, boolean sensitive) throws IOException {
            sendLock.lock();
            try {
                out.write(data);
                out.flush();
            } finally {
                sendLock.unlock();
            }
            if (log.isDebugEnabled()) {
                log.debug("TX -> {}", sensitive ? "<" + data.length + " bytes redacted>" : HEX.formatHex(data));
            }
        }

        @Override
        public void close() {
            closing = true;
            sessions.remove(this);
            try {
                socket.close();
            } catch (IOException e) {
                log.debug("Error closing market data socket", e);
            }
        }

        private void connect() throws IOException {
            socket.setKeepAlive(true);
            socket.setTcpNoDelay(true);
            socket.connect(new InetSocketAddress(properties.getHost(), port),
                    Math.toIntExact(properties.getConnectTimeout().toMillis()));
            socket.setSoTimeout(properties.getSocketTimeoutMs());
            out = new BufferedOutputStream(socket.getOutputStream());
            FrameReader reader = new FrameReader(new BufferedInputStream(socket.getInputStream()));
            Thread.ofVirtual().name("mitch-" + type.name().toLowerCase() + "-" + requestId).start(() -> listen(reader));
            log.info("{} request {}: connected to {}:{}", type, requestId, properties.getHost(), port);
        }

        private void login() throws IOException, InterruptedException {
            send(MitchEncoder.loginRequest(properties.getUsername(), properties.getPassword()), true);
            LoginResponse login = await(loginResponse, properties.getLoginTimeout(), "login response");
            if (!login.accepted()) {
                throw new MarketDataRejectedException("Login rejected: %s (status 0x%02X)"
                        .formatted(login.describe(), login.status()));
            }
            log.info("Logged in as [{}]: {}", properties.getUsername(), login.describe());
        }

        /** True when the completion marker arrives; false after {@code completion-timeout} without data. */
        private boolean awaitCompletion() throws IOException, InterruptedException {
            Duration idleTimeout = properties.getCompletionTimeout();
            while (true) {
                long remaining = lastActivityNanos + idleTimeout.toNanos() - System.nanoTime();
                if (remaining <= 0) {
                    log.warn("{} request {}: no completion marker and no data for {}; closing after {} records",
                            type, requestId, idleTimeout, records.get());
                    return false;
                }
                try {
                    streamCompleted.get(remaining, TimeUnit.NANOSECONDS);
                    log.info("{} request {} complete ({} records)", type, requestId, records.get());
                    return true;
                } catch (TimeoutException e) {
                    // data may have arrived meanwhile; re-check the idle deadline
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof SocketTimeoutException) {
                        log.warn("{} request {}: socket read timed out before the completion marker", type, requestId);
                        return false;
                    }
                    throw unwrap(e, "stream completion marker");
                }
            }
        }

        private void listen(FrameReader reader) {
            try {
                while (!closing) {
                    byte[] frame = reader.readFrame();
                    lastActivityNanos = System.nanoTime();
                    if (log.isTraceEnabled()) {
                        log.trace("RX <- {}", HEX.formatHex(frame));
                    }
                    if (requestSent) {
                        publish(new MarketDataFrameEvent(type, requestId, frame));
                    }
                    for (MitchMessage message : decoder.decode(frame)) {
                        handle(message);
                    }
                }
            } catch (EOFException e) {
                if (streamCompleted.isDone()) {
                    log.debug("{} request {}: connection closed by host after completion", type, requestId);
                } else if (!closing) {
                    log.error("{} request {}: connection closed by host before completion", type, requestId);
                }
                failPending(e);
            } catch (IOException | RuntimeException e) {
                if (!closing) {
                    log.error("{} request {}: listener stopped", type, requestId, e);
                }
                failPending(e);
            }
        }

        private void handle(MitchMessage message) {
            switch (message) {
                case LoginResponse response -> loginResponse.complete(response);
                case ReplayResponse response -> onReplayResponse(response);
                default -> {
                    records.incrementAndGet();
                    onData(message);
                }
            }
            publish(new MarketDataMessageEvent(type, requestId, message));
        }

        private void onData(MitchMessage message) {
            switch (message) {
                case SnapshotComplete complete -> {
                    log.info("End of snapshot for request id {}", complete.requestId());
                    streamCompleted.complete(null);
                }
                case InstrumentDefinition definition -> instrumentCache.put(definition.instrumentId(), definition.symbol());
                case SymbolDirectory directory -> instrumentCache.put(directory.instrumentId(), directory.symbol());
                case AddOrder order -> log.debug("Add order {} [{}]", order,
                        symbolFor(order.instrumentId()).orElse("UNKNOWN_" + order.instrumentId()));
                case SystemEvent event -> log.info("System event '{}' -> {}", event.eventCode(), event.describe());
                case HistoricalSymbol symbol -> log.debug("Historical symbol [{}]", symbol.symbol());
                case Malformed malformed -> log.warn("Malformed message: {}", malformed);
                default -> log.debug("RX {}", message);
            }
        }

        private void onReplayResponse(ReplayResponse response) {
            log.info("Replay response | request id: {}, channel: {}, group: {}, status: {} -> {}",
                    response.requestId(), response.channelId(), response.marketDataGroup(), response.status(),
                    response.describe());
            if (response.accepted()) {
                replayAccepted.complete(null);
            } else if (response.complete()) {
                replayAccepted.complete(null);
                streamCompleted.complete(null);
            } else {
                replayAccepted.completeExceptionally(
                        new MarketDataRejectedException("Replay request rejected: " + response.describe()));
            }
        }

        private void failPending(Exception cause) {
            loginResponse.completeExceptionally(cause);
            replayAccepted.completeExceptionally(cause);
            streamCompleted.completeExceptionally(cause);
        }
    }
}
