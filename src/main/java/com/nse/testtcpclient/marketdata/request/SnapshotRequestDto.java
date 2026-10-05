package com.nse.testtcpclient.marketdata.request;

/**
 * Payload of the SNAPSHOT_REQUEST topic; fields follow spec section 7.7.3 (Snapshot Request, 0x81).
 *
 * @param sequenceNumber  sequence number from which the client can build the order book
 * @param segment         up to 6 chars; blank if the request does not relate to a segment
 * @param symbol          up to 12 chars; blank if the request does not relate to an instrument
 * @param subBook         bit field: 0 Regular, 1 Off-Book, 2 Block Trade, 3 Odd Lot, 4 Early Settlement,
 *                        5 Bulletin Board, 6 Auction, 7 All or None
 * @param snapshotType    0 Order Book, 1 Symbol Status, 2 Instrument, 3 Trades, 4 Book-Level Statistics, 5 News,
 *                        6 Top of Book
 * @param recoverFromTime HH:MM:SS local time; only used for Trades (3) and News (5)
 */
public record SnapshotRequestDto(
        Integer requestID,
        Integer sequenceNumber,
        String segment,
        String symbol,
        Integer subBook,
        Integer snapshotType,
        String recoverFromTime) {
}
