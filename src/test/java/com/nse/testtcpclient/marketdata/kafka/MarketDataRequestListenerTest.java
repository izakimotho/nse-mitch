package com.nse.testtcpclient.marketdata.kafka;

import com.nse.testtcpclient.config.MitchProperties;
import com.nse.testtcpclient.marketdata.MarketDataClient;
import com.nse.testtcpclient.marketdata.MarketDataFrameEvent;
import com.nse.testtcpclient.marketdata.MarketDataRequestResult;
import com.nse.testtcpclient.marketdata.RequestType;
import com.nse.testtcpclient.marketdata.request.ReplayRequestDto;
import com.nse.testtcpclient.marketdata.request.SnapshotRequestDto;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@EmbeddedKafka(partitions = 1, topics = {"MARKET_REPLAY_REQUEST", "SNAPSHOT_REQUEST", "MARKET_REPLAY_RESPONSE"})
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

    @Test
    void publishesRawFramesToResponseTopicKeyedByRequestId(EmbeddedKafkaBroker broker) {
        byte[] frame = {12, 0, 1, 1, 9, 0, 0, 0, 4, 0, 4, 'C'};
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        KafkaAutoConfiguration.class, ConfigurationPropertiesAutoConfiguration.class))
                .withUserConfiguration(MitchProperties.class, MarketDataResponsePublisher.class)
                .withPropertyValues(
                        "spring.kafka.bootstrap-servers=" + broker.getBrokersAsString(),
                        "nse.mitch.replay.host=127.0.0.1",
                        "nse.mitch.replay.username=u",
                        "nse.mitch.replay.password=p")
                .run(context -> {
                    context.publishEvent(new MarketDataFrameEvent(RequestType.REPLAY, 305, frame));
                    context.publishEvent(new MarketDataRequestResult(
                            RequestType.REPLAY, 305, MarketDataRequestResult.Status.COMPLETED, 1, null));

                    Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("response-test", "false", broker);
                    try (Consumer<String, byte[]> consumer = new DefaultKafkaConsumerFactory<>(consumerProps,
                            new StringDeserializer(), new ByteArrayDeserializer()).createConsumer()) {
                        broker.consumeFromAnEmbeddedTopic(consumer, "MARKET_REPLAY_RESPONSE");
                        ConsumerRecord<String, byte[]> record =
                                KafkaTestUtils.getSingleRecord(consumer, "MARKET_REPLAY_RESPONSE");
                        assertThat(record.key()).isEqualTo("305");
                        assertThat(record.value()).isEqualTo(frame);
                        assertThat(new String(record.headers()
                                .lastHeader(MarketDataResponsePublisher.REQUEST_TYPE_HEADER).value(),
                                StandardCharsets.US_ASCII)).isEqualTo("REPLAY");
                    }
                });
    }
}
