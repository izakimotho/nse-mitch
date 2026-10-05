package com.nse.testtcpclient.marketdata;

/** Published once the initial replay/snapshot has finished and the client is consuming further messages. */
public record MarketDataSynchronizedEvent(long recordsReceived) {
}
