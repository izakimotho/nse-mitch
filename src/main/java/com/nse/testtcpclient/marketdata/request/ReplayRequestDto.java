package com.nse.testtcpclient.marketdata.request;

/** Payload of the MARKET_REPLAY_REQUEST topic. {@code requestID} is for correlation only; it is not sent on the wire. */
public record ReplayRequestDto(
        Integer count,
        Byte marketDataGroup,
        Integer startSequence,
        Integer requestID) {
}
