package com.nse.testtcpclient.marketdata.protocol;

import org.junit.jupiter.api.Test;

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
    void snapshotRequestMatchesOriginalLayout() {
        assertThat(HEX.formatHex(MitchEncoder.snapshotRequest(5001, 0, (byte) 4, (byte) 1)))
                .isEqualTo("1600" + "01" + "01" + "01000000" + "0e00" + "81"
                        + "89130000" + "00000000" + "04" + "01" + "00");
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
