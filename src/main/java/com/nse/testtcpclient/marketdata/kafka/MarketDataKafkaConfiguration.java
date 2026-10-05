package com.nse.testtcpclient.marketdata.kafka;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;

@Configuration(proxyBeanMethods = false)
public class MarketDataKafkaConfiguration {

    /** Converts JSON string payloads into the listener's parameter type. Spring Boot applies it to the default container factory. */
    @Bean
    @ConditionalOnMissingBean(RecordMessageConverter.class)
    public RecordMessageConverter jsonRecordMessageConverter() {
        return new StringJsonMessageConverter();
    }
}
