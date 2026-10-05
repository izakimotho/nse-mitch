package com.nse.testtcpclient.marketdata.protocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.nse.testtcpclient.marketdata.protocol.MitchMessage.*;
import static com.nse.testtcpclient.marketdata.protocol.MitchProtocol.*;

/** Stateless, thread-safe decoder from a full frame to its inner messages. */
public final class MitchDecoder {

    private static final Logger log = LoggerFactory.getLogger(MitchDecoder.class);
    private static final int PRICE_SCALE = 4;
    private static final int LONG_PRICE_SCALE = 8;

    private final boolean innerLengthIncludesLengthField;

    /**
     * @param innerLengthIncludesLengthField whether an inner message's u16 length counts its own two bytes
     *                                       (the spec says it does)
     */
    public MitchDecoder(boolean innerLengthIncludesLengthField) {
        this.innerLengthIncludesLengthField = innerLengthIncludesLengthField;
    }

    public List<MitchMessage> decode(byte[] frame) {
        if (frame.length < UNIT_HEADER_SIZE) {
            log.debug("Ignoring {}-byte frame shorter than the unit header", frame.length);
            return List.of();
        }
        if (isControlResponse(frame)) {
            // Read at a fixed offset so login/replay handling does not depend on the inner length convention.
            int type = frame[CONTROL_MESSAGE_TYPE_OFFSET] & 0xFF;
            int bodyStart = CONTROL_MESSAGE_TYPE_OFFSET + 1;
            return List.of(decodeSafely(type, body(frame, bodyStart, frame.length - bodyStart)));
        }
        return decodeMessages(frame);
    }

    private static boolean isControlResponse(byte[] frame) {
        if ((frame[MESSAGE_COUNT_OFFSET] & 0xFF) != SINGLE_MESSAGE || frame.length <= CONTROL_MESSAGE_TYPE_OFFSET) {
            return false;
        }
        int type = frame[CONTROL_MESSAGE_TYPE_OFFSET] & 0xFF;
        return type == LOGIN_RESPONSE || type == REPLAY_RESPONSE || type == SNAPSHOT_RESPONSE;
    }

    private List<MitchMessage> decodeMessages(byte[] frame) {
        List<MitchMessage> messages = new ArrayList<>();
        int position = UNIT_HEADER_SIZE;
        while (frame.length - position > LENGTH_FIELD_SIZE) {
            int innerLength = (frame[position] & 0xFF) | (frame[position + 1] & 0xFF) << 8;
            int end = position + innerLength + (innerLengthIncludesLengthField ? 0 : LENGTH_FIELD_SIZE);
            int bodyStart = position + LENGTH_FIELD_SIZE + 1;
            if (end < bodyStart || end > frame.length) {
                log.warn("Invalid inner length {} at offset {} of {}-byte frame; dropping remainder",
                        innerLength, position, frame.length);
                break;
            }
            int type = frame[position + LENGTH_FIELD_SIZE] & 0xFF;
            messages.add(decodeSafely(type, body(frame, bodyStart, end - bodyStart)));
            position = end;
        }
        return messages;
    }

    private static ByteBuffer body(byte[] frame, int offset, int length) {
        return ByteBuffer.wrap(frame, offset, length).slice().order(ByteOrder.LITTLE_ENDIAN);
    }

    private static MitchMessage decodeSafely(int type, ByteBuffer body) {
        int bodyLength = body.remaining();
        try {
            return decodeBody(type, body);
        } catch (BufferUnderflowException e) {
            log.warn("Message type 0x{} body too short ({} bytes)", hex(type), bodyLength);
            return new Malformed(type, "body too short: " + bodyLength + " bytes");
        }
    }

    private static MitchMessage decodeBody(int type, ByteBuffer body) {
        return switch (type) {
            case LOGIN_RESPONSE -> new LoginResponse(body.get() & 0xFF);
            case REPLAY_RESPONSE -> new ReplayResponse(body.getInt(), bigEndianUnsignedShort(body), body.get() & 0xFF,
                    (char) (body.get() & 0xFF));
            case SNAPSHOT_RESPONSE -> new SnapshotResponse(unsignedInt(body), unsignedInt(body),
                    (char) (body.get() & 0xFF), body.get() & 0xFF, body.getInt());
            case SNAPSHOT_COMPLETE -> new SnapshotComplete(unsignedInt(body), ascii(body, 6), ascii(body, 12),
                    body.get() & 0xFF, (char) (body.get() & 0xFF), body.get() & 0xFF, body.getInt());
            case TIME -> new TimeHeartbeat(unsignedInt(body));
            case SYSTEM_EVENT -> new SystemEvent(unsignedInt(body), (char) (body.get() & 0xFF));
            case SYMBOL_DIRECTORY -> new SymbolDirectory(unsignedInt(body), ascii(body, 12), character(body),
                    ascii(body, 12), ascii(body, 6), ascii(body, 8), ascii(body, 6), price(body), character(body),
                    ascii(body, 6), ascii(body, 8), price(body), unsignedByte(body), unsignedByte(body),
                    ascii(body, 5), longPrice(body));
            case ADD_ORDER -> new AddOrder(unsignedInt(body), body.getLong(), character(body), unsignedInt(body),
                    ascii(body, 12), price(body), unsignedByte(body), unsignedByte(body), unsignedByte(body),
                    price(body), unsignedByte(body));
            case ORDER_EXECUTED -> new OrderExecuted(unsignedInt(body), body.getLong(), unsignedInt(body),
                    body.getLong(), ascii(body, 6), ascii(body, 6), optionalLongPrice(body), optionalLongPrice(body),
                    optionalLongPrice(body));
            case CONSOLIDATED_STATISTICS -> new ConsolidatedStatistics(unsignedInt(body), unsignedByte(body),
                    ascii(body, 12), unsignedInt(body), longPrice(body), unsignedInt(body), longPrice(body),
                    longPrice(body), longPrice(body));
            case AON_INFO -> new AonInfo(unsignedInt(body), ascii(body, 12), price(body), character(body),
                    unsignedInt(body), character(body), ascii(body, 8));
            case TOP_OF_BOOK -> new TopOfBook(unsignedInt(body), ascii(body, 12), unsignedByte(body),
                    unsignedByte(body), character(body), price(body), unsignedInt(body), unsignedInt(body));
            default -> new Unknown(type, body.remaining());
        };
    }

    private static int unsignedByte(ByteBuffer buffer) {
        return buffer.get() & 0xFF;
    }

    private static char character(ByteBuffer buffer) {
        return (char) (buffer.get() & 0xFF);
    }

    private static BigDecimal price(ByteBuffer buffer) {
        return BigDecimal.valueOf(buffer.getInt(), PRICE_SCALE);
    }

    private static BigDecimal longPrice(ByteBuffer buffer) {
        return BigDecimal.valueOf(buffer.getLong(), LONG_PRICE_SCALE);
    }

    private static BigDecimal optionalLongPrice(ByteBuffer buffer) {
        return buffer.remaining() >= Long.BYTES ? longPrice(buffer) : null;
    }

    /** The replay response channel id is the one big-endian field in an otherwise little-endian body. */
    private static int bigEndianUnsignedShort(ByteBuffer buffer) {
        return Short.toUnsignedInt(Short.reverseBytes(buffer.getShort()));
    }

    private static long unsignedInt(ByteBuffer buffer) {
        return Integer.toUnsignedLong(buffer.getInt());
    }

    private static String ascii(ByteBuffer buffer, int length) {
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.US_ASCII).trim();
    }

    static String hex(int type) {
        return String.format("%02X", type);
    }
}
