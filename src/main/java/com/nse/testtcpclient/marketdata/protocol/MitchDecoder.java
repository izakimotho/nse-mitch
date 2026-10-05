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

    private final boolean innerLengthIncludesLengthField;

    /**
     * @param innerLengthIncludesLengthField whether an inner message's u16 length counts its own two bytes
     *                                       (outbound requests do; verify inbound against the venue spec)
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
        return type == LOGIN_RESPONSE || type == REPLAY_RESPONSE;
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
            case SNAPSHOT_COMPLETE, SNAPSHOT_COMPLETE_ALT -> new SnapshotComplete(body.getInt());
            case TIME -> new TimeHeartbeat(unsignedInt(body));
            case SYSTEM_EVENT -> new SystemEvent((char) (body.get() & 0xFF));
            case INSTRUMENT_DEFINITION -> new InstrumentDefinition(unsignedInt(body), ascii(body, 16));
            case SYMBOL_DIRECTORY -> new SymbolDirectory(unsignedInt(body), ascii(body, 12));
            case SYMBOL_DIRECTORY_HISTORICAL -> historicalSymbol(body);
            case ADD_ORDER -> new AddOrder(body.getLong(), unsignedInt(body), (char) (body.get() & 0xFF),
                    unsignedInt(body), BigDecimal.valueOf(body.getInt(), PRICE_SCALE));
            case ORDER_EXECUTED -> new OrderExecuted(body.getLong(), unsignedInt(body), unsignedInt(body));
            case SYSTEM_REGISTRY_TEXT -> new Text(type, "System Registry", ascii(body, body.remaining()));
            case ASSET_DEFINITION_TEXT -> new Text(type, "Asset Definition", ascii(body, body.remaining()));
            default -> new Unknown(type, body.remaining());
        };
    }

    private static HistoricalSymbol historicalSymbol(ByteBuffer body) {
        body.getInt(); // alignment flags, not an instrument id
        return new HistoricalSymbol(ascii(body, 12));
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
