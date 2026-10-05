package com.nse.testtcpclient.marketdata.request;

/** Payload of the SNAPSHOT_REQUEST topic. */
public record SnapshotRequestDto(
        Integer requestID,
        Integer instrumentId,
        Byte marketDataGroup,
        Byte snapshotType) {
}
