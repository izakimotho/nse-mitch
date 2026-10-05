package com.nse.testtcpclient.marketdata;

/** The gateway rejected the login or request. Not retried. */
public class MarketDataRejectedException extends MarketDataException {

    public MarketDataRejectedException(String message) {
        super(message);
    }
}
