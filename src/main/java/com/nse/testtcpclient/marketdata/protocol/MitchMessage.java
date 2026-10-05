package com.nse.testtcpclient.marketdata.protocol;

import java.math.BigDecimal;

/**
 * Decoded inner messages. Published as Spring application events by the client.
 * Application message layouts follow NSE Equities MITCH-UDP v1.22, section 7.9. Price fields have 4 implied decimals,
 * Long Price fields 8. Order and trade ids are UInt64 carried in a {@code long} (use {@link Long#toUnsignedString}).
 */
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

    /** Spec 7.9.3 (0x52). */
    record SymbolDirectory(long nanosecond, String symbol, char symbolStatus, String isin, String segment,
                           String expirationDate, String underlying, BigDecimal strikePrice, char optionType,
                           String issuer, String issueDate, BigDecimal coupon, int flags, int subBook,
                           String corporateAction, BigDecimal issuedQuantity) implements MitchMessage {
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

    /** Spec 7.9.5 (0x41). {@code settlementType}, {@code interestRate} and {@code term} apply to Block Trade only. */
    record AddOrder(long nanosecond, long orderId, char side, long quantity, String symbol, BigDecimal price,
                    int flags, int subBook, int settlementType, BigDecimal interestRate, int term)
            implements MitchMessage {
    }

    /** Spec 7.9.10 (0x45). The option fields are null if the gateway sends the pre-v1.16 39-byte message. */
    record OrderExecuted(long nanosecond, long orderId, long executedQuantity, long tradeId, String buyFirm,
                         String sellFirm, BigDecimal lastOptPx, BigDecimal volatility,
                         BigDecimal underlyingReferencePrice) implements MitchMessage {
    }

    /** Spec 7.9.21 (0x64). {@code statisticsType}: 0 Market, 1 Sector, 2 Instrument Type. */
    record ConsolidatedStatistics(long nanosecond, int statisticsType, String symbol, long volume,
                                  BigDecimal turnover, long trades, BigDecimal dailyForeignBuyValue,
                                  BigDecimal dailyForeignSellValue, BigDecimal marketCapitalization)
            implements MitchMessage {
    }

    /** Spec 7.9.23 (0x65). {@code aonStatus}: A Active, C Completed, H Halted, I Inactive, X Cancelled. */
    record AonInfo(long nanosecond, String symbol, BigDecimal price, char side, long quantity, char aonStatus,
                   String date) implements MitchMessage {
    }

    /** Spec 7.9.27 (0x71). {@code action}: 1 Update, 2 Delete. */
    record TopOfBook(long nanosecond, String symbol, int subBook, int action, char side, BigDecimal price,
                     long quantity, long marketOrderQuantity) implements MitchMessage {
    }

    record Unknown(int type, int bodyLength) implements MitchMessage {
    }

    record Malformed(int type, String reason) implements MitchMessage {
    }
}
