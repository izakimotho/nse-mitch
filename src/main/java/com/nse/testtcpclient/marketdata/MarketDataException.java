package com.nse.testtcpclient.marketdata;

import java.io.IOException;

public class MarketDataException extends IOException {

    public MarketDataException(String message) {
        super(message);
    }

    public MarketDataException(String message, Throwable cause) {
        super(message, cause);
    }
}
