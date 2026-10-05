package com.nse.testtcpclient.marketdata;

import com.nse.testtcpclient.config.MitchProperties;
import com.nse.testtcpclient.marketdata.MarketDataRequestResult.Status;
import com.nse.testtcpclient.marketdata.protocol.MitchMessage.AddOrder;
import com.nse.testtcpclient.marketdata.request.ReplayRequestDto;
import com.nse.testtcpclient.marketdata.request.SnapshotRequestDto;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.nse.testtcpclient.marketdata.protocol.TestFrames.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MarketDataClientTest {

    private static final HexFormat HEX = HexFormat.of();
    private static final String LOGIN_HEX = "1b000101010000001300014d44554b43426d697431323320202020";
    private static final String REPLAY_HEX = "16000101090000000e00030900000002000000000004";
    private static final ReplayRequestDto REPLAY = new ReplayRequestDto(2, (byte) 4, 9, 305);

    private final List<Object> events = new CopyOnWriteArrayList<>();
    private final List<FakeMitchServer> servers = new ArrayList<>();
    private MarketDataClient client;

    @AfterEach
    void tearDown() throws IOException {
        if (client != null) {
            client.stop();
        }
        for (FakeMitchServer server : servers) {
            server.close();
        }
    }

    @Test
    void replaySendsRequestParametersAndPublishesTaggedEvents() throws Exception {
        byte[] accepted = replayResponse('A');
        byte[] data = data(symbolDirectory(7, "SCOM"), historicalSymbol("KCB"), addOrder(123L, 7, 'B', 500, 152_500));
        byte[] complete = replayResponse('C');
        FakeMitchServer server = server(c -> {
            c.read();
            c.write(loginResponse('A'));
            c.read();
            c.write(accepted, data, complete);
        });
        client = client(properties(server.port(), 1, 1));

        MarketDataRequestResult result = client.replay(REPLAY);

        assertThat(result).isEqualTo(new MarketDataRequestResult(RequestType.REPLAY, 305, Status.COMPLETED, 3, null));
        assertThat(server.received).extracting(HEX::formatHex).containsExactly(LOGIN_HEX, REPLAY_HEX);
        assertThat(client.symbolFor(7)).contains("SCOM");
        assertThat(events).filteredOn(MarketDataMessageEvent.class::isInstance)
                .map(MarketDataMessageEvent.class::cast)
                .filteredOn(e -> e.message() instanceof AddOrder)
                .singleElement()
                .satisfies(e -> {
                    assertThat(e.type()).isEqualTo(RequestType.REPLAY);
                    assertThat(e.requestId()).isEqualTo(305);
                    assertThat(((AddOrder) e.message()).orderId()).isEqualTo(123L);
                });
        assertThat(events).filteredOn(MarketDataFrameEvent.class::isInstance)
                .map(MarketDataFrameEvent.class::cast)
                .allSatisfy(e -> assertThat(e.requestId()).isEqualTo(305))
                .extracting(MarketDataFrameEvent::frame)
                .containsExactly(accepted, data, complete);
        assertThat(events).contains(result);
        assertThat(server.errors).isEmpty();
    }

    @Test
    void missingMarketDataGroupFallsBackToConfiguredGroup() throws Exception {
        FakeMitchServer server = server(c -> {
            c.read();
            c.write(loginResponse('A'));
            c.read();
            c.write(replayResponse('C'));
        });
        client = client(properties(server.port(), 1, 1));

        client.replay(new ReplayRequestDto(2, null, 9, 305));

        assertThat(HEX.formatHex(server.received.get(1))).isEqualTo(REPLAY_HEX);
    }

    @Test
    void snapshotUsesSnapshotPortAndRequestFields() throws Exception {
        FakeMitchServer snapshotServer = server(c -> {
            c.read();
            c.write(loginResponse('A'));
            c.read();
            c.write(data(1, false, snapshotComplete(5001)));
        });
        client = client(properties(1, snapshotServer.port(), 1));

        MarketDataRequestResult result = client.snapshot(new SnapshotRequestDto(5001, 7, (byte) 4, (byte) 1));

        assertThat(result.status()).isEqualTo(Status.COMPLETED);
        assertThat(result.type()).isEqualTo(RequestType.SNAPSHOT);
        assertThat(HEX.formatHex(snapshotServer.received.get(1))).isEqualTo("16000101010000000e00818913000007000000040100");
    }

    @Test
    void rejectedLoginFailsWithoutRetrying() throws Exception {
        FakeMitchServer server = server(c -> {
            c.read();
            c.write(loginResponse('D'));
        });
        client = client(properties(server.port(), 1, 3));

        assertThatThrownBy(() -> client.replay(REPLAY))
                .isInstanceOf(MarketDataRejectedException.class)
                .hasMessageContaining("Login rejected: Denied (invalid credentials) (status 0x44)");
        assertThat(server.received).hasSize(1);
        assertThat(events).filteredOn(MarketDataRequestResult.class::isInstance).singleElement()
                .extracting(e -> ((MarketDataRequestResult) e).status()).isEqualTo(Status.FAILED);
    }

    @Test
    void rejectedReplayFails() throws Exception {
        FakeMitchServer server = server(c -> {
            c.read();
            c.write(loginResponse('A'));
            c.read();
            c.write(replayResponse('D'));
            c.read(); // hold the connection open until the client disconnects
        });
        client = client(properties(server.port(), 1, 3));

        assertThatThrownBy(() -> client.replay(REPLAY))
                .isInstanceOf(MarketDataRejectedException.class)
                .hasMessageContaining("Replay request rejected: Denied (invalid parameters)");
    }

    @Test
    void retriesWhenConnectionDropsBeforeAnyData() throws Exception {
        FakeMitchServer server = server(
                FakeMitchServer.Connection::read, // drop right after receiving the login
                c -> {
                    c.read();
                    c.write(loginResponse('A'));
                    c.read();
                    c.write(replayResponse('C'));
                });
        client = client(properties(server.port(), 1, 2));

        assertThat(client.replay(REPLAY).status()).isEqualTo(Status.COMPLETED);
        assertThat(server.received).hasSize(3);
    }

    @Test
    void incompleteWhenStreamGoesIdleWithoutCompletionMarker() throws Exception {
        FakeMitchServer server = server(c -> {
            c.read();
            c.write(loginResponse('A'));
            c.read();
            c.write(replayResponse('A'), data(symbolDirectory(7, "SCOM")));
            c.read(); // stay silent until the client disconnects
        });
        MitchProperties properties = properties(server.port(), 1, 1);
        properties.setCompletionTimeout(Duration.ofMillis(300));
        client = client(properties);

        MarketDataRequestResult result = client.replay(REPLAY);

        assertThat(result.status()).isEqualTo(Status.INCOMPLETE);
        assertThat(result.records()).isEqualTo(1);
    }

    @Test
    void stopAbortsInFlightRequest() throws Exception {
        CountDownLatch loginReceived = new CountDownLatch(1);
        FakeMitchServer server = server(c -> {
            c.read();
            loginReceived.countDown();
            c.read(); // never answer
        });
        client = client(properties(server.port(), 1, 3));

        CompletableFuture<MarketDataRequestResult> request = CompletableFuture.supplyAsync(() -> {
            try {
                return client.replay(REPLAY);
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        });
        assertThat(loginReceived.await(5, TimeUnit.SECONDS)).isTrue();
        client.stop();

        assertThatThrownBy(() -> request.get(5, TimeUnit.SECONDS))
                .cause().isInstanceOf(MarketDataException.class).hasMessageContaining("client stopped");
    }

    @Test
    void rejectsRequestWithMissingFields() {
        client = client(properties(1, 1, 1));

        assertThatThrownBy(() -> client.replay(new ReplayRequestDto(null, (byte) 4, 9, 305)))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("count is required");
        assertThatThrownBy(() -> client.snapshot(new SnapshotRequestDto(null, 0, (byte) 4, (byte) 1)))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("requestID is required");
    }

    private FakeMitchServer server(FakeMitchServer.Script... scripts) throws IOException {
        FakeMitchServer server = new FakeMitchServer(scripts);
        servers.add(server);
        return server;
    }

    private MarketDataClient client(MitchProperties properties) {
        properties.afterPropertiesSet();
        return new MarketDataClient(properties, events::add);
    }

    private static MitchProperties properties(int port, int snapshotPort, int maxAttempts) {
        MitchProperties properties = new MitchProperties();
        properties.setHost("127.0.0.1");
        properties.setPort(port);
        properties.setSnapshotPort(snapshotPort);
        properties.setUsername("MDUKCB");
        properties.setPassword("mit123");
        properties.setMarketDataGroup((byte) 4);
        properties.setSocketTimeoutMs(0);
        properties.setConnectTimeout(Duration.ofSeconds(2));
        properties.setLoginTimeout(Duration.ofSeconds(2));
        properties.setAcceptTimeout(Duration.ofSeconds(2));
        properties.setCompletionTimeout(Duration.ofSeconds(2));
        properties.setMaxConnectAttempts(maxAttempts);
        properties.setRetryBackoff(Duration.ofMillis(10));
        return properties;
    }
}
