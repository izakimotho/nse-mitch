package com.nse.testtcpclient.marketdata.protocol;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.Consumer;

/** Builds inbound frames for tests. A "message" is the type byte followed by its body. */
public final class TestFrames {

    private TestFrames() {
    }

    public static byte[] control(int type, byte[] body) {
        int inner = 2 + 1 + body.length;
        int length = 8 + inner;
        return ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN)
                .putShort((short) length).put((byte) 1).putInt(1).put((byte) 0)
                .putShort((short) inner).put((byte) type).put(body)
                .array();
    }

    public static byte[] data(int unitType, boolean inclusiveLength, byte[]... messages) {
        int length = 8 + Arrays.stream(messages).mapToInt(m -> 2 + m.length).sum();
        ByteBuffer buffer = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN)
                .putShort((short) length).put((byte) unitType).putInt(100).put((byte) 0);
        for (byte[] message : messages) {
            buffer.putShort((short) (inclusiveLength ? 2 + message.length : message.length)).put(message);
        }
        return buffer.array();
    }

    public static byte[] data(byte[]... messages) {
        return data(10, true, messages);
    }

    public static byte[] message(int type, int bodySize, Consumer<ByteBuffer> body) {
        ByteBuffer buffer = ByteBuffer.allocate(1 + bodySize).order(ByteOrder.LITTLE_ENDIAN).put((byte) type);
        body.accept(buffer);
        return buffer.array();
    }

    public static byte[] loginResponse(int status) {
        return control(0x02, new byte[]{(byte) status});
    }

    public static byte[] replayResponse(char status) {
        return control(0x04, ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(9).put((byte) 0).put((byte) 10).put((byte) 4).put((byte) status).array()); // channel 10, big-endian
    }

    public static byte[] symbolDirectory(long nanosecond, String symbol, char status) {
        return message(0x52, 87, b -> b.putInt((int) nanosecond).put(padded(symbol, 12)).put((byte) status)
                .put(padded("KE1000001402", 12)).put(padded("MBD", 6)).put(padded("", 8)).put(padded("", 6))
                .putInt(0).put((byte) ' ').put(padded("SCOM", 6)).put(padded("", 8)).putInt(0)
                .put((byte) 0).put((byte) 1).put(padded("", 5)).putLong(4_006_542_800_000_000_000L));
    }

    public static MitchMessage.SymbolDirectory expectedSymbolDirectory(long nanosecond, String symbol, char status) {
        return new MitchMessage.SymbolDirectory(nanosecond, symbol, status, "KE1000001402", "MBD", "", "",
                BigDecimal.valueOf(0, 4), ' ', "SCOM", "", BigDecimal.valueOf(0, 4), 0, 1, "",
                new BigDecimal("40065428000.00000000"));
    }

    public static byte[] snapshotResponse(char status, int requestId) {
        return control(0x82, ByteBuffer.allocate(14).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(50).putInt(2).put((byte) status).put((byte) 0).putInt(requestId).array());
    }

    public static byte[] addOrder(long orderId, char side, int quantity, String symbol, int rawPrice) {
        return message(0x41, 41, b -> b.putInt(1_000).putLong(orderId).put((byte) side).putInt(quantity)
                .put(padded(symbol, 12)).putInt(rawPrice).put((byte) 0).put((byte) 1).put((byte) 0).putInt(0)
                .put((byte) 0));
    }

    public static byte[] snapshotComplete(int requestId) {
        return message(0x83, 29, b -> b.putInt(50).put(padded("", 6)).put(padded("SCOM", 12))
                .put((byte) 1).put((byte) 'T').put((byte) 0).putInt(requestId));
    }

    public static MitchMessage.SnapshotComplete expectedSnapshotComplete(int requestId) {
        return new MitchMessage.SnapshotComplete(50, "", "SCOM", 1, 'T', 0, requestId);
    }

    public static byte[] padded(String value, int width) {
        byte[] bytes = new byte[width];
        Arrays.fill(bytes, (byte) ' ');
        byte[] raw = value.getBytes(StandardCharsets.US_ASCII);
        System.arraycopy(raw, 0, bytes, 0, raw.length);
        return bytes;
    }
}
