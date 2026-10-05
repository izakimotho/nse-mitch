package com.nse.testtcpclient.marketdata.kafka;

import com.nse.testtcpclient.marketdata.MarketDataClient;
import com.nse.testtcpclient.marketdata.MarketDataRequestResult;
import com.nse.testtcpclient.marketdata.request.ReplayRequestDto;
import com.nse.testtcpclient.marketdata.request.SnapshotRequestDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Runs each request from the replay and snapshot topics to completion before taking the next one. */
@Component
@ConditionalOnProperty(prefix = "nse.mitch.replay", name = "enabled", matchIfMissing = true)
public class MarketDataRequestListener {

    private static final Logger log = LoggerFactory.getLogger(MarketDataRequestListener.class);

    private final MarketDataClient client;

    public MarketDataRequestListener(MarketDataClient client) {
        this.client = client;
    }

    @KafkaListener(topics = "${nse.mitch.replay.replay-topic:MARKET_REPLAY_REQUEST}",
            groupId = "${nse.mitch.replay.kafka-group-id:nse-mitch-client}")
    public void onReplayRequest(ReplayRequestDto request) {
        log.info("Received replay request {}", request);
        process(request.requestID(), () -> client.replay(request));
    }

    @KafkaListener(topics = "${nse.mitch.replay.snapshot-topic:SNAPSHOT_REQUEST}",
            groupId = "${nse.mitch.replay.kafka-group-id:nse-mitch-client}")
    public void onSnapshotRequest(SnapshotRequestDto request) {
        log.info("Received snapshot request {}", request);
        process(request.requestID(), () -> client.snapshot(request));
    }

    private void process(Integer requestId, Request request) {
        try {
            MarketDataRequestResult result = request.run();
            log.info("Request {} finished: {}", requestId, result);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Request {} failed: {}", requestId, e.getMessage(), e);
        }
    }

    @FunctionalInterface
    private interface Request {
        MarketDataRequestResult run() throws Exception;
    }
}
