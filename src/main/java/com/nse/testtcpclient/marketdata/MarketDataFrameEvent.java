package com.nse.testtcpclient.marketdata;

/** A raw frame, exactly as received, from the session of a replay or snapshot request (after the request was sent). */
public record MarketDataFrameEvent(RequestType type, int requestId, byte[] frame) {

    @Override
    public String toString() {
        return "MarketDataFrameEvent[type=%s, requestId=%d, frame=%d bytes]".formatted(type, requestId, frame.length);
    }
}
