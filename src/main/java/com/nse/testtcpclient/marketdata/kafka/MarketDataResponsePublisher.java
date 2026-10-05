package com.nse.testtcpclient.marketdata.kafka;

import com.nse.testtcpclient.config.MitchProperties;
import com.nse.testtcpclient.marketdata.MarketDataFrameEvent;
import com.nse.testtcpclient.marketdata.MarketDataRequestResult;
import com.nse.testtcpclient.marketdata.RequestType;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Publishes every raw response frame to the replay or snapshot response topic, keyed by requestID. Uses its own
 * String/byte[] producer so the application's default {@code KafkaTemplate} is left untouched.
 */
@Component
@ConditionalOnProperty(prefix = "nse.mitch.replay", name = "enabled", matchIfMissing = true)
public class MarketDataResponsePublisher implements DisposableBean {

    public static final String REQUEST_TYPE_HEADER = "mitch-request-type";

    private static final Logger log = LoggerFactory.getLogger(MarketDataResponsePublisher.class);

    private final MitchProperties properties;
    private final DefaultKafkaProducerFactory<String, byte[]> producerFactory;
    private final KafkaTemplate<String, byte[]> template;

    public MarketDataResponsePublisher(MitchProperties properties, KafkaProperties kafkaProperties,
                                       ObjectProvider<SslBundles> sslBundles) {
        this.properties = properties;
        this.producerFactory = new DefaultKafkaProducerFactory<>(
                kafkaProperties.buildProducerProperties(sslBundles.getIfAvailable()),
                new StringSerializer(), new ByteArraySerializer());
        this.template = new KafkaTemplate<>(producerFactory);
    }

    @EventListener
    public void onFrame(MarketDataFrameEvent event) {
        String topic = event.type() == RequestType.REPLAY
                ? properties.getReplayResponseTopic()
                : properties.getSnapshotResponseTopic();
        ProducerRecord<String, byte[]> record =
                new ProducerRecord<>(topic, String.valueOf(event.requestId()), event.frame());
        record.headers().add(REQUEST_TYPE_HEADER, event.type().name().getBytes(StandardCharsets.US_ASCII));
        template.send(record).whenComplete((result, error) -> {
            if (error != null) {
                log.error("Failed to publish {} to {}", event, topic, error);
            }
        });
    }

    @EventListener
    public void onResult(MarketDataRequestResult result) {
        template.flush();
    }

    @Override
    public void destroy() {
        producerFactory.destroy();
    }
}
