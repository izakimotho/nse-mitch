package com.nse.testtcpclient.marketdata;

/** Outcome of one replay or snapshot request; also published as a Spring event. */
public record MarketDataRequestResult(RequestType type, int requestId, Status status, long records, String error) {

    public enum Status {
        /** The completion marker arrived. */
        COMPLETED,
        /** No completion marker and no data for {@code completion-timeout}. */
        INCOMPLETE,
        FAILED
    }
}
