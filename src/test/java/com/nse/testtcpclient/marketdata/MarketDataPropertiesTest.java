package com.nse.testtcpclient.marketdata;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class MarketDataPropertiesTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(MarketDataConfiguration.class, MarketDataClient.class);

    @Test
    void bindsPropertiesAndCreatesClient() {
        runner.withPropertyValues(
                        "marketdata.host=10.0.0.1",
                        "marketdata.username=MDUKCB",
                        "marketdata.password=secret",
                        "marketdata.sync-mode=snapshot",
                        "marketdata.login-timeout=3s",
                        "marketdata.replay.start-sequence=9",
                        "marketdata.replay.count=2",
                        "marketdata.replay.market-data-group=4",
                        "marketdata.snapshot.request-id=5001")
                .run(context -> {
                    assertThat(context).hasSingleBean(MarketDataClient.class);
                    MarketDataProperties properties = context.getBean(MarketDataProperties.class);
                    assertThat(properties.port()).isEqualTo(13496);
                    assertThat(properties.syncMode()).isEqualTo(MarketDataProperties.SyncMode.SNAPSHOT);
                    assertThat(properties.loginTimeout()).isEqualTo(Duration.ofSeconds(3));
                    assertThat(properties.replay()).isEqualTo(new MarketDataProperties.Replay(9, 2, (byte) 4));
                    assertThat(properties.snapshot().requestId()).isEqualTo(5001);
                    assertThat(properties.toString()).doesNotContain("secret");
                });
    }

    @Test
    void failsFastWhenHostIsMissing() {
        runner.withPropertyValues("marketdata.username=u", "marketdata.password=p")
                .run(context -> assertThat(context).hasFailed()
                        .getFailure().rootCause().hasMessageContaining("marketdata.host"));
    }

    @Test
    void connectionSettingsAreOptionalWhenDisabled() {
        runner.withPropertyValues("marketdata.enabled=false")
                .run(context -> assertThat(context).hasNotFailed());
    }
}
