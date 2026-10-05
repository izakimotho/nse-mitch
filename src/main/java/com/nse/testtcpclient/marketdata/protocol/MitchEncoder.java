package com.nse.testtcpclient.marketdata.protocol;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static com.nse.testtcpclient.marketdata.protocol.MitchProtocol.*;

/** Builds outbound control units. Inner lengths include the 2-byte length field. */
public final class MitchEncoder {

    static final int USERNAME_LENGTH = 6;
    static final int PASSWORD_LENGTH = 10;

    private MitchEncoder() {
    }

    public static byte[] loginRequest(String username, String password) {
        ByteBuffer buffer = controlUnit(1, LOGIN_REQUEST, USERNAME_LENGTH + PASSWORD_LENGTH);
        buffer.put(fixedAscii(username, USERNAME_LENGTH, "username"));
        buffer.put(fixedAscii(password, PASSWORD_LENGTH, "password"));
        return buffer.array();
    }

    public static byte[] replayRequest(int startSequence, int count, byte marketDataGroup) {
        ByteBuffer buffer = controlUnit(startSequence, REPLAY_REQUEST, 11);
        buffer.putInt(startSequence);
        buffer.putInt(count);
        buffer.putShort((short) 0); // channel
        buffer.put(marketDataGroup);
        return buffer.array();
    }

    public static byte[] snapshotRequest(int requestId, int instrumentId, byte marketDataGroup, byte snapshotType) {
        ByteBuffer buffer = controlUnit(1, SNAPSHOT_REQUEST, 11);
        buffer.putInt(requestId);
        buffer.putInt(instrumentId);
        buffer.put(marketDataGroup);
        buffer.put(snapshotType);
        buffer.put((byte) 0); // padding
        return buffer.array();
    }

    private static ByteBuffer controlUnit(int sequence, int messageType, int payloadLength) {
        int innerLength = LENGTH_FIELD_SIZE + 1 + payloadLength;
        int frameLength = UNIT_HEADER_SIZE + innerLength;
        return ByteBuffer.allocate(frameLength).order(ByteOrder.LITTLE_ENDIAN)
                .putShort((short) frameLength)
                .put((byte) SINGLE_MESSAGE)
                .put((byte) 1) // market data group
                .putInt(sequence)
                .putShort((short) innerLength)
                .put((byte) messageType);
    }

    private static byte[] fixedAscii(String value, int width, String field) {
        if (value == null) {
            throw new IllegalArgumentException(field + " must not be null");
        }
        if (!StandardCharsets.US_ASCII.newEncoder().canEncode(value)) {
            throw new IllegalArgumentException(field + " must be ASCII");
        }
        if (value.length() > width) {
            throw new IllegalArgumentException(field + " must be at most " + width + " characters");
        }
        byte[] padded = new byte[width];
        Arrays.fill(padded, (byte) ' ');
        byte[] raw = value.getBytes(StandardCharsets.US_ASCII);
        System.arraycopy(raw, 0, padded, 0, raw.length);
        return padded;
    }
}
