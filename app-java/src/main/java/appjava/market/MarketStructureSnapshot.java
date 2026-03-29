package appjava.market;

import java.util.List;

public record MarketStructureSnapshot(
    String symbol,
    String symbolName,
    String venueState,
    double lastPrice,
    double bidPrice,
    long bidQuantity,
    double askPrice,
    long askQuantity,
    double midPrice,
    double spread,
    double spreadBps,
    double indicativeOpenPrice,
    List<BookLevel> bids,
    List<BookLevel> asks,
    List<String> notes
) {
}
