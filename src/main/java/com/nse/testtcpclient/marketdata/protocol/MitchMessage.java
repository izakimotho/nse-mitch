package com.nse.testtcpclient.marketdata.protocol;

import java.math.BigDecimal;

/** Decoded inner messages. Published as Spring application events by the client. */
public sealed interface MitchMessage {

    record LoginResponse(int status) implements MitchMessage {
        public boolean accepted() {
            return status == 0x01 || status == 'A';
        }

        public String describe() {
            return switch (status) {
                case 'A' -> "Accepted";
                case 0x01 -> "Accepted (legacy code)";
                case 'D', 0x02 -> "Denied (invalid credentials)";
                case 0x03 -> "Denied (session already active)";
                default -> "Unknown status 0x%02X".formatted(status);
            };
        }
    }

    record ReplayResponse(int requestId, int channelId, int marketDataGroup, char status) implements MitchMessage {
        public boolean accepted() {
            return status == 'A';
        }

        public boolean complete() {
            return status == 'C';
        }

        public String describe() {
            return switch (status) {
                case 'A' -> "Accepted (replay processing started)";
                case 'D' -> "Denied (invalid parameters)";
                case 'C' -> "Completed (all historical packets sent)";
                default -> "Unknown status '" + status + "'";
            };
        }
    }

    /** Spec 7.8.4 (0x82): accept/reject reply to a snapshot request. */
    record SnapshotResponse(long sequenceNumber, long orderCount, char status, int snapshotType, int requestId)
            implements MitchMessage {
        public boolean accepted() {
            return status == 'A';
        }

        public String describe() {
            return switch (status) {
                case 'A' -> "Request Accepted";
                case 'O' -> "Out of Range";
                case 'U' -> "Snapshot Unavailable";
                case 'a' -> "Segment, Symbol or Sub Book Invalid or Not Specified";
                case 'b' -> "Request Limit Reached";
                case 'c' -> "Concurrent Limit Reached";
                case 'd' -> "Unsupported Message Type";
                case 'e' -> "Failed (Other)";
                default -> "Unknown status '" + status + "'";
            };
        }
    }

    /** Spec 7.8.5 (0x83): the snapshot has been fully sent. */
    record SnapshotComplete(long sequenceNumber, String segment, String symbol, int subBook, char tradingStatus,
                            int snapshotType, int requestId) implements MitchMessage {
    }

    record TimeHeartbeat(long secondsPastMidnight) implements MitchMessage {
    }

    /** Spec 7.9.2 (0x53). */
    record SystemEvent(long nanosecond, char eventCode) implements MitchMessage {
        public String describe() {
            return switch (eventCode) {
                case 'O' -> "Start of Day";
                case 'C' -> "End of Day";
                default -> "Unknown event code '" + eventCode + "'";
            };
        }
    }

    record InstrumentDefinition(long instrumentId, String symbol) implements MitchMessage {
    }

    /** Spec 7.9.3 (0x52). */
    record SymbolDirectory(long nanosecond, String symbol, char symbolStatus) implements MitchMessage {
        public String describeStatus() {
            return switch (symbolStatus) {
                case ' ' -> "Active";
                case 'H' -> "Halted";
                case 'S' -> "Suspended";
                case 'a' -> "Inactive";
                default -> "Unknown status '" + symbolStatus + "'";
            };
        }
    }

    /** Type 0x23: carries a symbol but no instrument id, so it is not cached. */
    record HistoricalSymbol(String symbol) implements MitchMessage {
    }

    record AddOrder(long orderId, long instrumentId, char side, long quantity, BigDecimal price) implements MitchMessage {
    }

    record OrderExecuted(long orderId, long executedQuantity, long matchId) implements MitchMessage {
    }

    record Text(int type, String context, String text) implements MitchMessage {
    }

    record Unknown(int type, int bodyLength) implements MitchMessage {
    }

    record Malformed(int type, String reason) implements MitchMessage {
    }
}
