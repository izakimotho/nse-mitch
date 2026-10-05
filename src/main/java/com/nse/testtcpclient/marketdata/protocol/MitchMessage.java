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

    record SnapshotComplete(int requestId) implements MitchMessage {
    }

    record TimeHeartbeat(long secondsPastMidnight) implements MitchMessage {
    }

    record SystemEvent(char eventCode) implements MitchMessage {
        public String describe() {
            return switch (eventCode) {
                case 'O' -> "Start of Day";
                case 'S' -> "Start of Session";
                case 'C' -> "End of Session";
                case 'E' -> "End of Day";
                case 'H' -> "Trading Halt";
                default -> "Unknown lifecycle code '" + eventCode + "'";
            };
        }
    }

    record InstrumentDefinition(long instrumentId, String symbol) implements MitchMessage {
    }

    record SymbolDirectory(long instrumentId, String symbol) implements MitchMessage {
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
