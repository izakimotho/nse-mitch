package com.nse.testtcpclient.marketdata.protocol;

/**
 * Wire constants for the MITCH-style protocol spoken by the NSE gateway.
 * Unit header (little-endian): {@code length:u16 | messageCount:u8 | marketDataGroup:u8 | sequence:u32}.
 * Inner message: {@code length:u16 | type:u8 | body}.
 */
public final class MitchProtocol {

    public static final int LENGTH_FIELD_SIZE = 2;
    public static final int UNIT_HEADER_SIZE = 8;
    public static final int MESSAGE_COUNT_OFFSET = 2;
    public static final int CONTROL_MESSAGE_TYPE_OFFSET = UNIT_HEADER_SIZE + LENGTH_FIELD_SIZE;
    public static final int SINGLE_MESSAGE = 1;

    public static final int LOGIN_REQUEST = 0x01;
    public static final int LOGIN_RESPONSE = 0x02;
    public static final int REPLAY_REQUEST = 0x03;
    public static final int REPLAY_RESPONSE = 0x04;
    public static final int SYMBOL_DIRECTORY_HISTORICAL = 0x23;
    public static final int ADD_ORDER = 0x41;
    public static final int ORDER_EXECUTED = 0x45;
    public static final int SYMBOL_DIRECTORY = 0x52;
    public static final int SYSTEM_EVENT = 0x53;
    public static final int TIME = 0x54;
    public static final int ASSET_DEFINITION_TEXT = 0x64;
    public static final int INSTRUMENT_DEFINITION = 0x65;
    public static final int SYSTEM_REGISTRY_TEXT = 0x71;
    public static final int SNAPSHOT_REQUEST = 0x81;
    public static final int SNAPSHOT_RESPONSE = 0x82;
    public static final int SNAPSHOT_COMPLETE = 0x83;

    private MitchProtocol() {
    }
}
