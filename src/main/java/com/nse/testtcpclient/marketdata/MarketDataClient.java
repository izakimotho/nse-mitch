package com.nse.testtcpclient.marketdata;

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
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Connects to the market data gateway, logs in, runs the configured replay/snapshot synchronization and then keeps
 * consuming messages. Every decoded {@link MitchMessage} is published as a Spring application event.
 */
@Component
public class MarketDataClient {

    private static final Logger log = LoggerFactory.getLogger(MarketDataClient.class);
    private static final HexFormat HEX = HexFormat.of();

    private final MarketDataProperties properties;
    private final ApplicationEventPublisher publisher;
    private final MitchDecoder decoder;
    private final Map<Long, String> instrumentCache = new ConcurrentHashMap<>();
    private final AtomicLong recordsReceived = new AtomicLong();

    private volatile boolean running;
    private volatile Thread supervisor;
    private volatile Session session;
    private volatile CompletableFuture<Void> synchronization = new CompletableFuture<>();

    public MarketDataClient(MarketDataProperties properties, ApplicationEventPublisher publisher) {
        this.properties = properties;
        this.publisher = publisher;
        this.decoder = new MitchDecoder(properties.innerLengthIncludesLengthField());
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        if (properties.enabled()) {
            start();
        } else {
            log.info("Market data client disabled (marketdata.enabled=false)");
        }
    }

    /** Starts connecting in the background; returns immediately. */
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        synchronization = new CompletableFuture<>();
        supervisor = Thread.ofVirtual().name("market-data-supervisor").start(this::run);
    }

    @PreDestroy
    public synchronized void stop() {
        running = false;
        Session current = session;
        if (current != null) {
            current.close();
        }
        Thread thread = supervisor;
        if (thread != null) {
            thread.interrupt();
        }
        synchronization.completeExceptionally(new CancellationException("Market data client stopped"));
    }

    /** Completes when the initial synchronization finishes; fails if every connection attempt fails. */
    public CompletableFuture<Void> synchronization() {
        return synchronization.copy();
    }

    public Optional<String> symbolFor(long instrumentId) {
        return Optional.ofNullable(instrumentCache.get(instrumentId));
    }

    public long recordsReceived() {
        return recordsReceived.get();
    }

    public boolean isRunning() {
        return running;
    }

    private void run() {
        try {
            runWithRetries();
        } finally {
            synchronized (this) {
                if (supervisor == Thread.currentThread()) {
                    running = false;
                }
            }
        }
    }

    private void runWithRetries() {
        int maxAttempts = properties.maxConnectAttempts();
        for (int attempt = 1; running; attempt++) {
            try (Session current = new Session()) {
                session = current;
                current.synchronize();
                long records = recordsReceived.get();
                log.info("Data synchronization complete ({} records). Consuming further messages.", records);
                synchronization.complete(null);
                publish(new MarketDataSynchronizedEvent(records));
                current.awaitClosed();
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                if (!running) {
                    return;
                }
                if (attempt >= maxAttempts) {
                    log.error("Market data synchronization failed after {} attempt(s)", attempt, e);
                    synchronization.completeExceptionally(e);
                    return;
                }
                log.warn("Market data attempt {}/{} failed: {}. Retrying in {}",
                        attempt, maxAttempts, e.toString(), properties.retryBackoff());
            } finally {
                session = null;
            }
            if (!sleep(properties.retryBackoff())) {
                return;
            }
        }
    }

    private static boolean sleep(Duration duration) {
        try {
            Thread.sleep(duration);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void publish(Object event) {
        try {
            publisher.publishEvent(event);
        } catch (RuntimeException e) {
            log.error("Market data event listener failed for {}", event, e);
        }
    }

    /** One TCP connection. Never reused: a retry creates a new session. */
    private final class Session implements AutoCloseable {

        private final Socket socket = new Socket();
        private final ReentrantLock sendLock = new ReentrantLock();
        private final CompletableFuture<LoginResponse> loginResponse = new CompletableFuture<>();
        private final CompletableFuture<Void> replayAccepted = new CompletableFuture<>();
        private final CompletableFuture<Void> streamCompleted = new CompletableFuture<>();
        private final CompletableFuture<Void> closed = new CompletableFuture<>();
        private volatile boolean closing;
        private OutputStream out;

        void synchronize() throws IOException, InterruptedException, TimeoutException {
            connect();
            send(MitchEncoder.loginRequest(properties.username(), properties.password()), true);
            LoginResponse login = await(loginResponse, properties.loginTimeout(), "login response");
            if (!login.accepted()) {
                throw new MarketDataException("Login rejected: %s (status 0x%02X)"
                        .formatted(login.describe(), login.status()));
            }
            log.info("Logged in as [{}]: {}", properties.username(), login.describe());

            switch (properties.syncMode()) {
                case REPLAY -> {
                    MarketDataProperties.Replay replay = properties.replay();
                    log.info("Requesting replay | start seq: {}, count: {}, group: {}",
                            replay.startSequence(), replay.count(), replay.marketDataGroup());
                    send(MitchEncoder.replayRequest(replay.startSequence(), replay.count(), replay.marketDataGroup()),
                            false);
                    await(replayAccepted, properties.acceptTimeout(), "replay acceptance");
                    awaitCompletion();
                }
                case SNAPSHOT -> {
                    MarketDataProperties.Snapshot snapshot = properties.snapshot();
                    log.info("Requesting snapshot | request id: {}, instrument: {}, group: {}, type: {}",
                            snapshot.requestId(), snapshot.instrumentId(), snapshot.marketDataGroup(),
                            snapshot.snapshotType());
                    send(MitchEncoder.snapshotRequest(snapshot.requestId(), snapshot.instrumentId(),
                            snapshot.marketDataGroup(), snapshot.snapshotType()), false);
                    awaitCompletion();
                }
                case NONE -> log.info("Sync mode NONE: skipping replay/snapshot");
            }
        }

        void awaitClosed() throws InterruptedException, ExecutionException {
            closed.get();
        }

        @Override
        public void close() {
            closing = true;
            try {
                socket.close();
            } catch (IOException e) {
                log.debug("Error closing market data socket", e);
            }
        }

        private void connect() throws IOException {
            socket.setKeepAlive(true);
            socket.setTcpNoDelay(true);
            socket.connect(new InetSocketAddress(properties.host(), properties.port()),
                    toMillis(properties.connectTimeout()));
            socket.setSoTimeout(toMillis(properties.readTimeout()));
            out = new BufferedOutputStream(socket.getOutputStream());
            FrameReader reader = new FrameReader(new BufferedInputStream(socket.getInputStream()));
            Thread.ofVirtual().name("market-data-listener").start(() -> listen(reader));
            log.info("Connected to {}:{}", properties.host(), properties.port());
        }

        private void awaitCompletion() throws IOException, InterruptedException {
            log.info("Waiting for stream completion marker...");
            try {
                await(streamCompleted, properties.completionTimeout(), "stream completion marker");
            } catch (TimeoutException e) {
                log.warn("{}; continuing without it", e.getMessage());
            }
        }

        private void send(byte[] data, boolean sensitive) throws IOException {
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

        private void listen(FrameReader reader) {
            try {
                while (!closing) {
                    byte[] frame = reader.readFrame();
                    if (log.isTraceEnabled()) {
                        log.trace("RX <- {}", HEX.formatHex(frame));
                    }
                    for (MitchMessage message : decoder.decode(frame)) {
                        handle(message);
                    }
                }
            } catch (EOFException e) {
                if (streamCompleted.isDone()) {
                    log.info("Connection closed by host after stream completion");
                } else if (!closing) {
                    log.error("Connection closed by host before stream completion");
                }
                failPending(e);
            } catch (IOException | RuntimeException e) {
                if (!closing) {
                    log.error("Market data listener stopped", e);
                }
                failPending(e);
            } finally {
                closed.complete(null);
            }
        }

        private void handle(MitchMessage message) {
            recordsReceived.incrementAndGet();
            switch (message) {
                case LoginResponse response -> loginResponse.complete(response);
                case ReplayResponse response -> onReplayResponse(response);
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
            publish(message);
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
                        new MarketDataException("Replay request rejected: " + response.describe()));
            }
        }

        private void failPending(Exception cause) {
            loginResponse.completeExceptionally(cause);
            replayAccepted.completeExceptionally(cause);
            streamCompleted.completeExceptionally(cause);
        }

        private static <T> T await(CompletableFuture<T> future, Duration timeout, String what)
                throws IOException, InterruptedException, TimeoutException {
            try {
                return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                throw new TimeoutException("Timed out after " + timeout + " waiting for " + what);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof IOException io) {
                    throw io;
                }
                throw new MarketDataException("Failed waiting for " + what, e.getCause());
            }
        }

        private static int toMillis(Duration duration) {
            return Math.toIntExact(duration.toMillis());
        }
    }
}
