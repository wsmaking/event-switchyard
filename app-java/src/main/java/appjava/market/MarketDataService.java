package appjava.market;

import appjava.http.JsonHttpHandler;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public final class MarketDataService {
    private static final long FRESHNESS_BUDGET_MS = 3_000L;
    private final Map<String, SymbolSpec> specs = Map.of(
        "7203", new SymbolSpec("トヨタ自動車", 2500.0, 1.0, 4.0, 10.0),
        "6758", new SymbolSpec("ソニーグループ", 13500.0, 5.0, 5.0, 12.0),
        "9984", new SymbolSpec("ソフトバンクグループ", 6200.0, 1.0, 6.0, 12.0),
        "6861", new SymbolSpec("キーエンス", 52000.0, 10.0, 6.5, 14.0),
        "8306", new SymbolSpec("三菱UFJフィナンシャル・グループ", 1200.0, 0.5, 3.5, 8.0)
    );
    private final Map<String, StockInfo> latest = new ConcurrentHashMap<>();
    private final Map<String, ArrayDeque<PricePoint>> history = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "app-java-market-data");
        thread.setDaemon(true);
        return thread;
    });

    public MarketDataService() {
        reset();
        scheduler.scheduleAtFixedRate(this::tickAll, 1, 1, TimeUnit.SECONDS);
    }

    public synchronized void reset() {
        latest.clear();
        history.clear();
        specs.forEach((symbol, spec) -> latest.put(symbol, seed(symbol, spec)));
        latest.forEach((symbol, stockInfo) -> appendHistory(symbol, stockInfo.currentPrice()));
    }

    public StockInfo getStockInfo(String symbol) {
        StockInfo stockInfo = latest.get(symbol);
        if (stockInfo == null) {
            throw new JsonHttpHandler.NotFoundException("symbol_not_found:" + symbol);
        }
        return stockInfo;
    }

    public List<PricePoint> getPriceHistory(String symbol, int limit) {
        if (!latest.containsKey(symbol)) {
            throw new JsonHttpHandler.NotFoundException("symbol_not_found:" + symbol);
        }
        ArrayDeque<PricePoint> points = history.get(symbol);
        if (points == null) {
            return List.of();
        }
        int safeLimit = Math.max(1, limit);
        return points.stream()
            .skip(Math.max(0, points.size() - safeLimit))
            .toList();
    }

    public double getCurrentPrice(String symbol) {
        return getStockInfo(symbol).currentPrice();
    }

    public MarketStructureSnapshot getMarketStructure(String symbol) {
        StockInfo stockInfo = getStockInfo(symbol);
        SymbolSpec spec = specs.get(symbol);
        double lastPrice = stockInfo.currentPrice();
        double halfSpread = Math.max(spec.tickSize(), round2(spec.tickSize() * (1.0 + Math.abs(stockInfo.changePercent()) / 2.0)));
        double bidPrice = applyTick(Math.max(spec.tickSize(), lastPrice - halfSpread), spec.tickSize());
        double askPrice = applyTick(lastPrice + halfSpread, spec.tickSize());
        if (askPrice <= bidPrice) {
            askPrice = applyTick(bidPrice + spec.tickSize(), spec.tickSize());
        }
        double midPrice = round2((bidPrice + askPrice) / 2.0);
        double spread = round2(askPrice - bidPrice);
        double spreadBps = midPrice == 0.0 ? 0.0 : round2((spread / midPrice) * 10_000.0);
        long baseDepth = Math.max(100L, stockInfo.volume() / 1_200L);
        List<BookLevel> bids = List.of(
            new BookLevel(bidPrice, baseDepth),
            new BookLevel(applyTick(bidPrice - spec.tickSize(), spec.tickSize()), Math.max(50L, Math.round(baseDepth * 0.82))),
            new BookLevel(applyTick(bidPrice - spec.tickSize() * 2.0, spec.tickSize()), Math.max(50L, Math.round(baseDepth * 0.67)))
        );
        List<BookLevel> asks = List.of(
            new BookLevel(askPrice, Math.max(50L, Math.round(baseDepth * 0.95))),
            new BookLevel(applyTick(askPrice + spec.tickSize(), spec.tickSize()), Math.max(50L, Math.round(baseDepth * 0.76))),
            new BookLevel(applyTick(askPrice + spec.tickSize() * 2.0, spec.tickSize()), Math.max(50L, Math.round(baseDepth * 0.61)))
        );
        String venueState = determineVenueState(stockInfo, spec);
        double indicativeOpenPrice = round2(applyTick(midPrice + (spec.tickSize() * 2.0), spec.tickSize()));
        List<String> notes = switch (venueState) {
            case "AUCTION_WATCH" -> List.of("寄り付き・引け前後の板寄せを意識する局面", "指値の価格保護と約定待ちの説明が重要");
            case "HALT_WATCH" -> List.of("値動きが大きく、約定より状態説明が先に必要な局面", "再送より先に venue 状態確認を優先");
            default -> List.of("通常の連続約定フェーズ", "spread と queue priority を見る基本局面");
        };
        return new MarketStructureSnapshot(
            symbol,
            stockInfo.name(),
            venueState,
            round2(lastPrice),
            round2(bidPrice),
            bids.getFirst().quantity(),
            round2(askPrice),
            asks.getFirst().quantity(),
            midPrice,
            spread,
            spreadBps,
            indicativeOpenPrice,
            bids,
            asks,
            notes
        );
    }

    public synchronized MarketFeedRuntimeSnapshot getRuntimeSnapshot() {
        long generatedAt = Instant.now().toEpochMilli();
        List<MarketFeedSymbolRuntime> symbols = specs.keySet().stream()
            .sorted()
            .map(symbol -> {
                StockInfo stockInfo = latest.get(symbol);
                if (stockInfo == null) {
                    return null;
                }
                ArrayDeque<PricePoint> points = history.get(symbol);
                PricePoint lastPoint = points == null ? null : points.peekLast();
                long lastTickAt = lastPoint == null ? generatedAt : lastPoint.timestamp();
                long tickAgeMs = Math.max(0L, generatedAt - lastTickAt);
                MarketStructureSnapshot structure = getMarketStructure(symbol);
                return new MarketFeedSymbolRuntime(
                    symbol,
                    stockInfo.name(),
                    lastTickAt,
                    tickAgeMs,
                    structure.venueState(),
                    structure.midPrice(),
                    structure.spreadBps()
                );
            })
            .filter(java.util.Objects::nonNull)
            .toList();
        long maxTickAgeMs = symbols.stream()
            .mapToLong(MarketFeedSymbolRuntime::tickAgeMs)
            .max()
            .orElse(0L);
        int staleSymbolCount = (int) symbols.stream()
            .filter(symbol -> symbol.tickAgeMs() > FRESHNESS_BUDGET_MS)
            .count();
        int venueAlertCount = (int) symbols.stream()
            .filter(symbol -> !"CONTINUOUS".equalsIgnoreCase(symbol.venueState()))
            .count();
        String state = staleSymbolCount > 0
            ? "STALE"
            : venueAlertCount > 0
            ? "DEGRADED"
            : "FRESH";
        return new MarketFeedRuntimeSnapshot(
            generatedAt,
            state,
            FRESHNESS_BUDGET_MS,
            maxTickAgeMs,
            staleSymbolCount,
            venueAlertCount,
            symbols
        );
    }

    private void tickAll() {
        specs.forEach((symbol, spec) -> latest.compute(symbol, (key, previous) -> next(symbol, spec, previous)));
    }

    private StockInfo seed(String symbol, SymbolSpec spec) {
        return new StockInfo(symbol, spec.name, spec.basePrice, 0.0, 0.0, spec.basePrice, spec.basePrice, 100_000L);
    }

    private StockInfo next(String symbol, SymbolSpec spec, StockInfo previous) {
        double lastPrice = previous == null ? spec.basePrice : previous.currentPrice();
        double moveBps = ThreadLocalRandom.current().nextDouble(-spec.volatilityBps(), spec.volatilityBps());
        double unclamped = lastPrice * (1.0 + moveBps / 10_000.0);
        double min = spec.basePrice() * (1.0 - spec.maxMovePercent() / 100.0);
        double max = spec.basePrice() * (1.0 + spec.maxMovePercent() / 100.0);
        double currentPrice = applyTick(clamp(unclamped, min, max), spec.tickSize());
        double change = currentPrice - spec.basePrice();
        double changePercent = spec.basePrice() == 0.0 ? 0.0 : (change / spec.basePrice()) * 100.0;
        double high = previous == null ? currentPrice : Math.max(previous.high(), currentPrice);
        double low = previous == null ? currentPrice : Math.min(previous.low(), currentPrice);
        long volume = previous == null ? 100_000L : previous.volume() + ThreadLocalRandom.current().nextLong(500L, 4000L);
        appendHistory(symbol, currentPrice);
        return new StockInfo(
            symbol,
            spec.name(),
            round2(currentPrice),
            round2(change),
            round2(changePercent),
            round2(high),
            round2(low),
            volume
        );
    }

    private void appendHistory(String symbol, double price) {
        history.computeIfAbsent(symbol, ignored -> new ArrayDeque<>())
            .addLast(new PricePoint(Instant.now().toEpochMilli(), round2(price)));
        ArrayDeque<PricePoint> deque = history.get(symbol);
        while (deque.size() > 300) {
            deque.removeFirst();
        }
    }

    private static double clamp(double value, double min, double max) {
        return Math.max(min, Math.min(max, value));
    }

    private static double applyTick(double value, double tickSize) {
        return Math.round(value / tickSize) * tickSize;
    }

    private static double round2(double value) {
        return Math.round(value * 100.0) / 100.0;
    }

    private static String determineVenueState(StockInfo stockInfo, SymbolSpec spec) {
        double absoluteMove = Math.abs(stockInfo.changePercent());
        if (absoluteMove >= spec.maxMovePercent() * 0.75) {
            return "HALT_WATCH";
        }
        if (absoluteMove >= spec.maxMovePercent() * 0.45) {
            return "AUCTION_WATCH";
        }
        return "CONTINUOUS";
    }

    private record SymbolSpec(String name, double basePrice, double tickSize, double volatilityBps, double maxMovePercent) {
    }

    public record MarketFeedRuntimeSnapshot(
        long generatedAt,
        String state,
        long freshnessBudgetMs,
        long maxTickAgeMs,
        int staleSymbolCount,
        int venueAlertCount,
        List<MarketFeedSymbolRuntime> symbols
    ) {
    }

    public record MarketFeedSymbolRuntime(
        String symbol,
        String symbolName,
        long lastTickAt,
        long tickAgeMs,
        String venueState,
        double midPrice,
        double spreadBps
    ) {
    }
}
