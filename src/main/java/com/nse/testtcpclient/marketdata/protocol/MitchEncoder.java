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
    static final int SEGMENT_LENGTH = 6;
    static final int SYMBOL_LENGTH = 12;
    static final int TIME_LENGTH = 8;

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

    /** Spec 7.7.3: 39-byte Snapshot Request. Blank (null) alpha fields are sent as spaces. */
    public static byte[] snapshotRequest(int sequenceNumber, String segment, String symbol, int subBook,
                                         int snapshotType, String recoverFromTime, int requestId) {
        ByteBuffer buffer = controlUnit(1, SNAPSHOT_REQUEST, 4 + SEGMENT_LENGTH + SYMBOL_LENGTH + 1 + 1 + TIME_LENGTH + 4);
        buffer.putInt(sequenceNumber);
        buffer.put(fixedAscii(blankIfNull(segment), SEGMENT_LENGTH, "segment"));
        buffer.put(fixedAscii(blankIfNull(symbol), SYMBOL_LENGTH, "symbol"));
        buffer.put(unsignedByte(subBook, "subBook"));
        buffer.put(unsignedByte(snapshotType, "snapshotType"));
        buffer.put(fixedAscii(blankIfNull(recoverFromTime), TIME_LENGTH, "recoverFromTime"));
        buffer.putInt(requestId);
        return buffer.array();
    }

    private static String blankIfNull(String value) {
        return value == null ? "" : value;
    }

    private static byte unsignedByte(int value, String field) {
        if (value < 0 || value > 0xFF) {
            throw new IllegalArgumentException(field + " must be between 0 and 255");
        }
        return (byte) value;
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
