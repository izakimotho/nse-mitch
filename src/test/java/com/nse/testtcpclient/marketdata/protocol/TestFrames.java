package com.nse.testtcpclient.marketdata.protocol;

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
        return data(10, false, messages);
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

    public static byte[] symbolDirectory(long instrumentId, String symbol) {
        return message(0x52, 16, b -> b.putInt((int) instrumentId).put(padded(symbol, 12)));
    }

    public static byte[] addOrder(long orderId, long instrumentId, char side, int quantity, int rawPrice) {
        return message(0x41, 21, b -> b.putLong(orderId).putInt((int) instrumentId).put((byte) side)
                .putInt(quantity).putInt(rawPrice));
    }

    public static byte[] historicalSymbol(String symbol) {
        return message(0x23, 16, b -> b.putInt(0x0BADF00D).put(padded(symbol, 12)));
    }

    public static byte[] snapshotComplete(int requestId) {
        return message(0x82, 4, b -> b.putInt(requestId));
    }

    public static byte[] padded(String value, int width) {
        byte[] bytes = new byte[width];
        Arrays.fill(bytes, (byte) ' ');
        byte[] raw = value.getBytes(StandardCharsets.US_ASCII);
        System.arraycopy(raw, 0, bytes, 0, raw.length);
        return bytes;
    }
}
