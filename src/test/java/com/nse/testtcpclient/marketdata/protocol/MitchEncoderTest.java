package com.nse.testtcpclient.marketdata.protocol;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class MitchEncoderTest {

    private static final HexFormat HEX = HexFormat.of();

    // Login bytes equal the known-good array in the original class comment: {27,0,1,1,1,0,0,0,19,0,1,77,...}.
    @Test
    void loginRequestMatchesOriginalLayout() {
        assertThat(HEX.formatHex(MitchEncoder.loginRequest("MDUKCB", "mit123")))
                .isEqualTo("1b00" + "01" + "01" + "01000000" + "1300" + "01"
                        + "4d44554b4342" + "6d697431323320202020");
    }

    @Test
    void replayRequestMatchesOriginalLayout() {
        assertThat(HEX.formatHex(MitchEncoder.replayRequest(4, 4, (byte) 45)))
                .isEqualTo("1600" + "01" + "01" + "04000000" + "0e00" + "03"
                        + "04000000" + "04000000" + "0000" + "2d");
    }

    @Test
    void snapshotRequestMatchesSpecLayout() {
        assertThat(HEX.formatHex(MitchEncoder.snapshotRequest(9, "EQTY", "KCB", 1, 3, "09:30:00", 77)))
                .isEqualTo("2f00" + "01" + "01" + "01000000"         // unit header: length 47, 1 msg, group 1, seq 1
                        + "2700" + "81"                              // length 39, type 0x81
                        + "09000000"                                 // sequence number
                        + ascii("EQTY  ")                            // segment (6)
                        + ascii("KCB         ")                      // symbol (12)
                        + "01" + "03"                                // sub book, snapshot type
                        + ascii("09:30:00")                          // recover from time (8)
                        + "4d000000");                               // request id
    }

    @Test
    void snapshotRequestSendsBlankAlphaFieldsAsSpaces() {
        byte[] packet = MitchEncoder.snapshotRequest(0, null, null, 1, 0, null, 1);
        assertThat(packet).hasSize(47);
        assertThat(HEX.formatHex(packet, 15, 43)).isEqualTo("20".repeat(18) + "01" + "00" + "20".repeat(8));
    }

    @Test
    void snapshotRequestRejectsInvalidFields() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> MitchEncoder.snapshotRequest(0, null, "THIRTEENCHARS", 1, 0, null, 1))
                .withMessageContaining("symbol");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> MitchEncoder.snapshotRequest(0, null, null, 256, 0, null, 1))
                .withMessageContaining("subBook");
    }

    private static String ascii(String value) {
        return HEX.formatHex(value.getBytes(StandardCharsets.US_ASCII));
    }

    @Test
    void rejectsCredentialsThatDoNotFit() {
        assertThatIllegalArgumentException().isThrownBy(() -> MitchEncoder.loginRequest("TOOLONGUSER", "pw"))
                .withMessageContaining("username");
        assertThatIllegalArgumentException().isThrownBy(() -> MitchEncoder.loginRequest("user", "password-too-long"))
                .withMessageContaining("password");
        assertThatIllegalArgumentException().isThrownBy(() -> MitchEncoder.loginRequest("usér", "pw"))
                .withMessageContaining("ASCII");
    }
}
