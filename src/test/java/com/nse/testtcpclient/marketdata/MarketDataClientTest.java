package com.nse.testtcpclient.marketdata;

import com.nse.testtcpclient.marketdata.MarketDataProperties.SyncMode;
import com.nse.testtcpclient.marketdata.protocol.MitchMessage;
import com.nse.testtcpclient.marketdata.protocol.MitchMessage.AddOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.nse.testtcpclient.marketdata.protocol.TestFrames.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MarketDataClientTest {

    private static final HexFormat HEX = HexFormat.of();

    private final List<Object> events = new CopyOnWriteArrayList<>();
    private MarketDataClient client;
    private FakeMitchServer server;

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) {
            client.stop();
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    void replaySynchronizesCachesSymbolsAndPublishesEvents() throws Exception {
        server = new FakeMitchServer(c -> {
            c.read();
            c.write(loginResponse('A'));
            c.read();
            c.write(replayResponse('A'),
                    data(symbolDirectory(7, "SCOM"), historicalSymbol("KCB"), addOrder(123L, 7, 'B', 500, 152_500)),
                    replayResponse('C'));
        });
        client = client(SyncMode.REPLAY, 1);

        client.start();
        client.synchronization().get(5, TimeUnit.SECONDS);

        assertThat(server.received).extracting(HEX::formatHex).containsExactly(
                "1b000101010000001300014d44554b43426d697431323320202020",
                "16000101090000000e00030900000002000000000004");
        assertThat(client.symbolFor(7)).contains("SCOM");
        assertThat(events).filteredOn(AddOrder.class::isInstance).singleElement()
                .extracting(e -> ((AddOrder) e).orderId()).isEqualTo(123L);
        assertThat(events).filteredOn(MarketDataSynchronizedEvent.class::isInstance).hasSize(1);
        assertThat(server.errors).isEmpty();
    }

    @Test
    void snapshotCompletesOnSnapshotCompleteMarker() throws Exception {
        server = new FakeMitchServer(c -> {
            c.read();
            c.write(loginResponse('A'));
            c.read();
            c.write(data(1, false, snapshotComplete(5001)));
        });
        client = client(SyncMode.SNAPSHOT, 1);

        client.start();
        client.synchronization().get(5, TimeUnit.SECONDS);

        assertThat(HEX.formatHex(server.received.get(1))).startsWith("1600010101000000" + "0e00" + "81");
        assertThat(events).contains(new MitchMessage.SnapshotComplete(5001));
    }

    @Test
    void rejectedLoginFailsSynchronization() throws Exception {
        server = new FakeMitchServer(c -> {
            c.read();
            c.write(loginResponse('D'));
        });
        client = client(SyncMode.REPLAY, 1);

        client.start();

        assertThatThrownBy(() -> client.synchronization().get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .cause().isInstanceOf(MarketDataException.class).hasMessageContaining("Login rejected: Denied (invalid credentials) (status 0x44)");
    }

    @Test
    void rejectedReplayFailsSynchronization() throws Exception {
        server = new FakeMitchServer(c -> {
            c.read();
            c.write(loginResponse('A'));
            c.read();
            c.write(replayResponse('D'));
            c.read(); // hold the connection open until the client disconnects
        });
        client = client(SyncMode.REPLAY, 1);

        client.start();

        assertThatThrownBy(() -> client.synchronization().get(5, TimeUnit.SECONDS))
                .cause().hasMessageContaining("Replay request rejected: Denied (invalid parameters)");
    }

    @Test
    void retriesWhenConnectionDropsDuringSynchronization() throws Exception {
        server = new FakeMitchServer(
                FakeMitchServer.Connection::read, // drop right after receiving the login
                c -> {
                    c.read();
                    c.write(loginResponse('A'));
                    c.read();
                    c.write(replayResponse('C'));
                });
        client = client(SyncMode.REPLAY, 2);

        client.start();
        client.synchronization().get(5, TimeUnit.SECONDS);

        assertThat(server.received).hasSize(3);
    }

    @Test
    void stopCancelsPendingSynchronization() throws Exception {
        CountDownLatch loginReceived = new CountDownLatch(1);
        server = new FakeMitchServer(c -> {
            c.read();
            loginReceived.countDown();
            c.read(); // never answer
        });
        client = client(SyncMode.REPLAY, 1);

        client.start();
        assertThat(loginReceived.await(5, TimeUnit.SECONDS)).isTrue();
        client.stop();

        assertThatThrownBy(() -> client.synchronization().get(5, TimeUnit.SECONDS))
                .cause().isInstanceOf(CancellationException.class);
    }

    private MarketDataClient client(SyncMode mode, int maxAttempts) {
        MarketDataProperties properties = new MarketDataProperties(true, "127.0.0.1", server.port(), "MDUKCB", "mit123",
                Duration.ofSeconds(2), Duration.ZERO, Duration.ofSeconds(2), Duration.ofSeconds(2),
                Duration.ofSeconds(2), maxAttempts, Duration.ofMillis(10), false, mode,
                new MarketDataProperties.Replay(9, 2, (byte) 4),
                new MarketDataProperties.Snapshot(5001, 0, (byte) 4, (byte) 1));
        return new MarketDataClient(properties, events::add);
    }
}
