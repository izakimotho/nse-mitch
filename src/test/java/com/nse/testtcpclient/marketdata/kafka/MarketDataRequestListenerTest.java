package com.nse.testtcpclient.marketdata.kafka;

import com.nse.testtcpclient.marketdata.MarketDataClient;
import com.nse.testtcpclient.marketdata.request.ReplayRequestDto;
import com.nse.testtcpclient.marketdata.request.SnapshotRequestDto;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@EmbeddedKafka(partitions = 1, topics = {"MARKET_REPLAY_REQUEST", "SNAPSHOT_REQUEST"})
class MarketDataRequestListenerTest {

    @Test
    @SuppressWarnings("unchecked")
    void consumesJsonRequestsFromBothTopics(EmbeddedKafkaBroker broker) {
        MarketDataClient client = mock(MarketDataClient.class);
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class))
                .withUserConfiguration(MarketDataKafkaConfiguration.class, MarketDataRequestListener.class)
                .withBean(MarketDataClient.class, () -> client)
                .withPropertyValues(
                        "spring.kafka.bootstrap-servers=" + broker.getBrokersAsString(),
                        "spring.kafka.consumer.auto-offset-reset=earliest")
                .run(context -> {
                    KafkaTemplate<Object, Object> template = context.getBean(KafkaTemplate.class);
                    template.send("MARKET_REPLAY_REQUEST",
                            "{\"count\":4,\"marketDataGroup\":4,\"startSequence\":4,\"requestID\":305}");
                    template.send("SNAPSHOT_REQUEST",
                            "{\"requestID\":5001,\"instrumentId\":0,\"marketDataGroup\":4,\"snapshotType\":1}");

                    verify(client, timeout(30_000)).replay(new ReplayRequestDto(4, (byte) 4, 4, 305));
                    verify(client, timeout(30_000)).snapshot(new SnapshotRequestDto(5001, 0, (byte) 4, (byte) 1));
                });
    }
}
