package com.nse.testtcpclient.marketdata;

import com.nse.testtcpclient.config.MitchProperties;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class MitchPropertiesTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ConfigurationPropertiesAutoConfiguration.class))
            .withUserConfiguration(MitchProperties.class, MarketDataClient.class);

    @Test
    void bindsPropertiesAndCreatesClient() {
        runner.withPropertyValues(
                        "nse.mitch.replay.host=10.0.0.1",
                        "nse.mitch.replay.snapshot-port=13497",
                        "nse.mitch.replay.username=MDUKCB",
                        "nse.mitch.replay.password=secret",
                        "nse.mitch.replay.market-data-group=4",
                        "nse.mitch.replay.sync-mode=snapshot",
                        "nse.mitch.replay.login-timeout=3s",
                        "nse.mitch.replay.replay.start-sequence=9",
                        "nse.mitch.replay.replay.count=2",
                        "nse.mitch.replay.snapshot.request-id=5001")
                .run(context -> {
                    assertThat(context).hasSingleBean(MarketDataClient.class);
                    MitchProperties properties = context.getBean(MitchProperties.class);
                    assertThat(properties.getPort()).isEqualTo(13496);
                    assertThat(properties.connectPort()).isEqualTo(13497);
                    assertThat(properties.getMarketDataGroup()).isEqualTo((byte) 4);
                    assertThat(properties.getSocketTimeoutMs()).isEqualTo(10_000);
                    assertThat(properties.getSyncMode()).isEqualTo(MitchProperties.SyncMode.SNAPSHOT);
                    assertThat(properties.getLoginTimeout()).isEqualTo(Duration.ofSeconds(3));
                    assertThat(properties.getReplay().getStartSequence()).isEqualTo(9);
                    assertThat(properties.getReplay().getCount()).isEqualTo(2);
                    assertThat(properties.getSnapshot().getRequestId()).isEqualTo(5001);
                    assertThat(properties.toString()).doesNotContain("secret");
                });
    }

    @Test
    void failsFastWhenHostIsMissing() {
        runner.withPropertyValues("nse.mitch.replay.username=u", "nse.mitch.replay.password=p")
                .run(context -> assertThat(context).hasFailed()
                        .getFailure().rootCause().hasMessageContaining("nse.mitch.replay.host"));
    }

    @Test
    void connectionSettingsAreOptionalWhenDisabled() {
        runner.withPropertyValues("nse.mitch.replay.enabled=false")
                .run(context -> assertThat(context).hasNotFailed());
    }
}
