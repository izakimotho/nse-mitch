package com.nse.testtcpclient.marketdata;

import com.nse.testtcpclient.marketdata.protocol.MitchMessage;

/** A decoded message, tagged with the request whose session received it. */
public record MarketDataMessageEvent(RequestType type, int requestId, MitchMessage message) {
}
