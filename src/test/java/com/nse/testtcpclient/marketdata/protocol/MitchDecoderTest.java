package com.nse.testtcpclient.marketdata.protocol;

import com.nse.testtcpclient.marketdata.protocol.MitchMessage.AddOrder;
import com.nse.testtcpclient.marketdata.protocol.MitchMessage.AonInfo;
import com.nse.testtcpclient.marketdata.protocol.MitchMessage.ConsolidatedStatistics;
import com.nse.testtcpclient.marketdata.protocol.MitchMessage.OrderExecuted;
import com.nse.testtcpclient.marketdata.protocol.MitchMessage.TopOfBook;
import com.nse.testtcpclient.marketdata.protocol.MitchMessage.SystemEvent;
import com.nse.testtcpclient.marketdata.protocol.MitchMessage.LoginResponse;
import com.nse.testtcpclient.marketdata.protocol.MitchMessage.Malformed;
import com.nse.testtcpclient.marketdata.protocol.MitchMessage.ReplayResponse;
import com.nse.testtcpclient.marketdata.protocol.MitchMessage.SnapshotResponse;
import com.nse.testtcpclient.marketdata.protocol.MitchMessage.SymbolDirectory;
import com.nse.testtcpclient.marketdata.protocol.MitchMessage.Unknown;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.nse.testtcpclient.marketdata.protocol.TestFrames.*;
import static org.assertj.core.api.Assertions.assertThat;

class MitchDecoderTest {

    private final MitchDecoder decoder = new MitchDecoder(true);

    @ParameterizedTest
    @ValueSource(ints = {0x01, 'A'})
    void loginAccepted(int status) {
        assertThat(decoder.decode(loginResponse(status)))
                .singleElement().isEqualTo(new LoginResponse(status))
                .satisfies(m -> assertThat(((LoginResponse) m).accepted()).isTrue());
    }

    @Test
    void loginRejected() {
        LoginResponse denied = (LoginResponse) decoder.decode(loginResponse('D')).getFirst();
        LoginResponse alreadyActive = (LoginResponse) decoder.decode(loginResponse(0x03)).getFirst();
        assertThat(denied.accepted()).isFalse();
        assertThat(denied.describe()).isEqualTo("Denied (invalid credentials)");
        assertThat(alreadyActive.describe()).isEqualTo("Denied (session already active)");
    }

    @Test
    void replayResponses() {
        ReplayResponse accepted = (ReplayResponse) decoder.decode(replayResponse('A')).getFirst();
        ReplayResponse complete = (ReplayResponse) decoder.decode(replayResponse('C')).getFirst();
        assertThat(accepted).isEqualTo(new ReplayResponse(9, 10, 4, 'A'));
        assertThat(accepted.accepted()).isTrue();
        assertThat(complete.complete()).isTrue();
    }

    @Test
    void decodesEveryMessageInABundle() {
        byte[] frame = data(symbolDirectory(7, "SCOM", ' '), addOrder(123L, 'B', 500, "SCOM", 152_500),
                snapshotComplete(5001));

        assertThat(decoder.decode(frame)).containsExactly(
                expectedSymbolDirectory(7, "SCOM", ' '),
                new AddOrder(1_000, 123L, 'B', 500, "SCOM", new BigDecimal("15.2500"), 0, 1, 0,
                        BigDecimal.valueOf(0, 4), 0),
                expectedSnapshotComplete(5001));
    }

    @Test
    void exclusiveLengthConventionIsSupported() {
        byte[] frame = data(10, false, symbolDirectory(7, "SCOM", 'H'), snapshotComplete(5001));

        assertThat(new MitchDecoder(false).decode(frame))
                .containsExactly(expectedSymbolDirectory(7, "SCOM", 'H'), expectedSnapshotComplete(5001));
    }

    @Test
    void singleMessageDataUnitWithUnitTypeOneIsNotDropped() {
        byte[] frame = data(1, true, addOrder(1L, 'S', 10, "KCB", 10_000));

        assertThat(decoder.decode(frame)).singleElement().isInstanceOf(AddOrder.class);
    }

    @Test
    void shortBodyIsIsolatedAndDecodingContinues() {
        byte[] truncatedAddOrder = message(0x41, 3, b -> b.put(new byte[3]));
        byte[] frame = data(truncatedAddOrder, snapshotComplete(1));

        List<MitchMessage> messages = decoder.decode(frame);

        assertThat(messages).hasSize(2);
        assertThat(messages.get(0)).isInstanceOf(Malformed.class);
        assertThat(messages.get(1)).isEqualTo(expectedSnapshotComplete(1));
    }

    @Test
    void unknownTypesAreReportedAndSkipped() {
        byte[] frame = data(message(0x99, 5, b -> b.put(new byte[5])), snapshotComplete(1));

        assertThat(decoder.decode(frame)).containsExactly(new Unknown(0x99, 5), expectedSnapshotComplete(1));
    }

    @Test
    void invalidInnerLengthStopsAtThatMessage() {
        byte[] frame = data(snapshotComplete(1), snapshotComplete(2));
        frame[8 + 2 + snapshotComplete(1).length] = (byte) 0xFF; // corrupt the second message's length

        assertThat(decoder.decode(frame)).containsExactly(expectedSnapshotComplete(1));
    }

    @Test
    void orderExecuted() {
        byte[] full = message(0x45, 60, b -> b.putInt(5).putLong(123L).putInt(200).putLong(-1L)
                .put(padded("BRK01", 6)).put(padded("BRK02", 6)).putLong(150_000_000L).putLong(0L).putLong(-1L));
        byte[] legacy = message(0x45, 36, b -> b.putInt(5).putLong(123L).putInt(200).putLong(9L)
                .put(padded("", 6)).put(padded("", 6)));

        assertThat(decoder.decode(data(full, legacy))).containsExactly(
                new OrderExecuted(5, 123L, 200, -1L, "BRK01", "BRK02", new BigDecimal("1.50000000"),
                        BigDecimal.valueOf(0, 8), BigDecimal.valueOf(-1, 8)),
                new OrderExecuted(5, 123L, 200, 9L, "", "", null, null, null));
    }

    @Test
    void consolidatedStatistics() {
        byte[] frame = data(message(0x64, 57, b -> b.putInt(5).put((byte) 1).put(padded("BANKING", 12))
                .putInt(1_000).putLong(2_500_000_000L).putInt(42).putLong(100_000_000L).putLong(0L).putLong(-1L)));

        assertThat(decoder.decode(frame)).containsExactly(new ConsolidatedStatistics(5, 1, "BANKING", 1_000,
                new BigDecimal("25.00000000"), 42, new BigDecimal("1.00000000"), BigDecimal.valueOf(0, 8),
                BigDecimal.valueOf(-1, 8)));
    }

    @Test
    void aonInfo() {
        byte[] frame = data(message(0x65, 34, b -> b.putInt(5).put(padded("SCOM", 12)).putInt(152_500)
                .put((byte) 'S').putInt(10_000).put((byte) 'A').put(padded("20261005", 8))));

        assertThat(decoder.decode(frame)).containsExactly(
                new AonInfo(5, "SCOM", new BigDecimal("15.2500"), 'S', 10_000, 'A', "20261005"));
    }

    @Test
    void topOfBook() {
        byte[] frame = data(message(0x71, 33, b -> b.putInt(5).put(padded("SCOM", 12)).put((byte) 1)
                .put((byte) 1).put((byte) 'B').putInt(152_500).putInt(3_000).putInt(0).putShort((short) 0)));

        assertThat(decoder.decode(frame)).containsExactly(
                new TopOfBook(5, "SCOM", 1, 1, 'B', new BigDecimal("15.2500"), 3_000, 0));
    }

    @Test
    void type0x23IsNotInTheSpec() {
        byte[] frame = data(message(0x23, 16, b -> b.put(new byte[16])));

        assertThat(decoder.decode(frame)).containsExactly(new Unknown(0x23, 16));
    }

    @Test
    void systemEventDescriptions() {
        byte[] frame = data(message(0x53, 5, b -> b.putInt(1_000).put((byte) 'C')));

        SystemEvent event = (SystemEvent) decoder.decode(frame).getFirst();
        assertThat(event).isEqualTo(new SystemEvent(1_000, 'C'));
        assertThat(event.describe()).isEqualTo("End of Day");
    }

    @Test
    void snapshotResponses() {
        SnapshotResponse accepted = (SnapshotResponse) decoder.decode(snapshotResponse('A', 5001)).getFirst();
        SnapshotResponse unavailable = (SnapshotResponse) decoder.decode(snapshotResponse('U', 5001)).getFirst();
        assertThat(accepted).isEqualTo(new SnapshotResponse(50, 2, 'A', 0, 5001));
        assertThat(accepted.accepted()).isTrue();
        assertThat(unavailable.accepted()).isFalse();
        assertThat(unavailable.describe()).isEqualTo("Snapshot Unavailable");
    }

    @Test
    void symbolDirectoryStatus() {
        SymbolDirectory halted = (SymbolDirectory) decoder.decode(data(symbolDirectory(1, "KCB", 'H'))).getFirst();
        SymbolDirectory active = (SymbolDirectory) decoder.decode(data(symbolDirectory(1, "KCB", ' '))).getFirst();
        assertThat(halted.describeStatus()).isEqualTo("Halted");
        assertThat(active.describeStatus()).isEqualTo("Active");
    }

    @Test
    void framesShorterThanHeaderAreIgnored() {
        assertThat(decoder.decode(new byte[]{4, 0, 1, 0})).isEmpty();
    }
}
