# NSE market data client (refactored)

Drop-in replacement for `com.nse.testtcpclient.tests.MarketDataClient`. Requires Java 21 and Spring Boot 3.

Copy `src/main/java/com/nse/testtcpclient/marketdata/**` and `config/MitchProperties.java` into your project, delete the
old `MarketDataClient`, `UnitHeader` and `ReplayRequestDto`, and add the `nse.mitch.replay` block from
`src/main/resources/application.yml` to your config (credentials via env vars).

## Layout

| Class | Role |
|---|---|
| `protocol/FrameReader` | Reads length-prefixed frames, validates length, handles partial reads |
| `protocol/MitchEncoder` | Builds login / replay / snapshot requests (byte-identical to the original) |
| `protocol/MitchDecoder` | Pure frame -> `List<MitchMessage>` decoder; per-message isolation |
| `protocol/MitchMessage` | Sealed interface of decoded records (`AddOrder`, `SymbolDirectory`, ...) |
| `MarketDataClient` | Connection lifecycle, login, sync, retries, symbol cache, event publishing |
| `config/MitchProperties` | `nse.mitch.replay.*` configuration, validated at startup |

## Consuming data

Every decoded message is published as a Spring event:

```java
@EventListener
void onAddOrder(MitchMessage.AddOrder order) { ... }

@EventListener
void onSynchronized(MarketDataSynchronizedEvent event) { ... }
```

Or wait programmatically: `marketDataClient.synchronization().get()`.

## Wire format

Unit header (little-endian): `length:u16 | messageCount:u8 | marketDataGroup:u8 | sequence:u32`, matching the known-good
login bytes `{27,0,1,1,1,0,0,0,19,0,1,...}`. `MitchEncoder` replaces `UnitHeader`, `buildLoginPacket`,
`buildReplayPacket` and `ReplayRequestDto`; replay parameters come from `nse.mitch.replay.*`.

## Verify against the venue spec

* `nse.mitch.replay.inner-length-includes-length-field` (default `false`, matching the original decoder). Outbound
  requests count the 2-byte length field; if inbound bundles do too, set this to `true`.
* Add Order price is decoded as a 4-byte int with 4 implied decimals (`BigDecimal`), as in the original.

## Build

```
mvn test
```
