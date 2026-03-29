package appjava.asset;

import java.util.List;

public final class AssetClassCatalogService {
    public AssetClassCatalog snapshot() {
        return new AssetClassCatalog(
            System.currentTimeMillis(),
            List.of(
                new AssetClassDefinition(
                    "Equities",
                    "DMA / care / algo",
                    "accepted -> working -> partial/fill/cancel/expire",
                    "last / bid-ask / spread / board depth",
                    "T+2 cash equities settlement",
                    "single-name concentration、beta、liquidity",
                    List.of("board lot / odd lot", "auction / halt", "corporate action"),
                    List.of("order identity", "audit trail", "OMS / BackOffice split"),
                    List.of("board depth", "settlement date", "corporate action mapping"),
                    List.of(
                        new ExampleMetric("sample fill", "100,000株 @ 2,842.5円", "arrival benchmark と平均約定単価の差をそのまま execution quality に使う"),
                        new ExampleMetric("settlement cash", "-284,250,000円", "buy side では gross cash out が settlement base"),
                        new ExampleMetric("risk lens", "single-name 41.6%", "concentration と liquidity が主因")
                    ),
                    List.of("trade date", "settlement date", "record date / ex-date"),
                    List.of("asset master と reporting label を分ける", "final-out と statement を同義にしない"),
                    List.of("queue priority を見ずに market 化する", "dividend / split を position history に直書きする")
                ),
                new AssetClassDefinition(
                    "Options",
                    "listed option order / quote",
                    "quote -> order -> fill -> greek refresh -> exercise / assignment",
                    "vol surface、skew、time to expiry",
                    "premium settlement + exercise workflow",
                    "delta / gamma / vega / theta",
                    List.of("series selection", "expiry roll", "exercise cutoff"),
                    List.of("order lifecycle", "audit trail", "position truth"),
                    List.of("valuation engine", "greeks refresh", "exercise event"),
                    List.of(
                        new ExampleMetric("premium", "42.8円 x 10枚", "premium cashflow と underlying を別管理する"),
                        new ExampleMetric("delta-equivalent", "325株相当", "hedge intuition は株数換算で語る"),
                        new ExampleMetric("theta", "-0.62 / 日", "時間経過で premium が削れる")
                    ),
                    List.of("trade date", "premium settlement", "exercise / assignment cutoff"),
                    List.of("premium cashflow と underlying position を別帳票で持つ", "exercise 前後で risk driver が変わる"),
                    List.of("現物の average price だけで説明しようとする", "expiry cutoff を UI の時刻だけで持つ")
                ),
                new AssetClassDefinition(
                    "FX",
                    "RFQ / stream quote",
                    "quote -> fill -> cash ladder -> nostro confirmation",
                    "spot / forward points / carry",
                    "pair ごとの dual-currency settlement",
                    "spot exposure、carry、basis",
                    List.of("session liquidity", "cutoff time", "holiday calendar"),
                    List.of("audit trail", "trade capture", "operator action trace"),
                    List.of("dual-currency ledger", "calendar logic", "cutoff management"),
                    List.of(
                        new ExampleMetric("spot trade", "USD/JPY 2M @ 151.20", "base/quote の二本 leg を同時に持つ"),
                        new ExampleMetric("cash legs", "+2,000,000 USD / -302,400,000 JPY", "片側だけの ledger に押し込まない"),
                        new ExampleMetric("cutoff", "Tokyo 15:30", "cutoff 超過は next-day settlement へ波及")
                    ),
                    List.of("trade date", "value date", "holiday-adjusted settlement"),
                    List.of("currency pair ごとに受渡 leg を分ける", "cash ladder を position view と同一にしない"),
                    List.of("single-currency ledger に押し込む", "holiday / cutoff を venue time と混同する")
                ),
                new AssetClassDefinition(
                    "Rates",
                    "RFQ / voice assisted order",
                    "quote -> trade -> accrual -> curve refresh -> settlement",
                    "yield curve、duration、convexity",
                    "coupon / accrual / day-count convention",
                    "DV01、curve shock、basis",
                    List.of("day count", "holiday convention", "coupon schedule"),
                    List.of("trade capture", "audit trail", "position truth"),
                    List.of("curve build", "cashflow schedule", "accrual engine"),
                    List.of(
                        new ExampleMetric("clean price", "99.42", "clean / dirty price を分けて扱う"),
                        new ExampleMetric("accrued", "0.31", "coupon accrual は settlement 後追いにしない"),
                        new ExampleMetric("DV01", "18,400 USD/bp", "risk は price ではなく rate sensitivity で見る")
                    ),
                    List.of("trade date", "coupon accrual", "settlement convention"),
                    List.of("price と yield を同時に保持する", "statement と risk で curve version を揃える"),
                    List.of("equity price のまま評価する", "coupon accrual を settlement と同時点でしか持たない")
                ),
                new AssetClassDefinition(
                    "Credit",
                    "axes / RFQ",
                    "quote -> trade -> event watch -> settlement",
                    "spread、hazard、recovery",
                    "coupon / default event / reference obligation convention",
                    "spread widening、jump-to-default",
                    List.of("liquidity pockets", "reference obligation", "event handling"),
                    List.of("trade capture", "allocation", "audit"),
                    List.of("default workflow", "valuation source hierarchy", "event engine"),
                    List.of(
                        new ExampleMetric("spread", "235bp", "price より spread と recovery 前提で語る"),
                        new ExampleMetric("jump-to-default", "-12.4M円", "single event で PnL が飛ぶ"),
                        new ExampleMetric("coupon accrual", "0.44", "credit でも accrual は別線で残す")
                    ),
                    List.of("trade date", "coupon accrual", "credit event watch"),
                    List.of("default / restructuring を order lifecycle と別 workflow で持つ", "books and records は event 後の連続性を重視する"),
                    List.of("spread product を equity と同じ stale guard でしか見ない", "credit event を symbol rename と同列に扱う")
                ),
                new AssetClassDefinition(
                    "Futures",
                    "listed futures order",
                    "accepted -> fill -> variation margin -> roll",
                    "front / next curve、basis",
                    "daily settlement + margin",
                    "basis、roll、liquidity concentration",
                    List.of("session break", "roll schedule", "variation margin"),
                    List.of("order state", "audit trail", "position truth"),
                    List.of("contract calendar", "margin workflow", "roll logic"),
                    List.of(
                        new ExampleMetric("contract", "TOPIX JUN26 x 120枚", "contract calendar を symbol と分けて持つ"),
                        new ExampleMetric("variation margin", "+4,320,000円", "daily settlement が現物と決定的に違う"),
                        new ExampleMetric("initial margin", "18,000,000円", "execution 後に margin workflow が主役になる")
                    ),
                    List.of("trade date", "daily settlement", "roll date"),
                    List.of("daily settlement と realized PnL を明示的に切る", "front contract と next contract を同じ symbol に畳まない"),
                    List.of("cash equities の受渡モデルを流用する", "variation margin を statement 後追いにする")
                )
            ),
            List.of(
                "受注、状態遷移、監査の骨格は共通化できる",
                "valuation、risk driver、settlement convention は asset class ごとに専用化する",
                "books and records の列は似ていても、埋まる意味は asset class ごとに違う",
                "equities の mental model を他商品へそのまま移植しない"
            )
        );
    }

    public record AssetClassCatalog(
        long generatedAt,
        List<AssetClassDefinition> assetClasses,
        List<String> boundaryPrinciples
    ) {
    }

    public record AssetClassDefinition(
        String assetClass,
        String tradeInitiation,
        String lifecycle,
        String valuationDriver,
        String settlementModel,
        String riskDriver,
        List<String> operatorWatchpoints,
        List<String> whatStaysCommon,
        List<String> whatMustSpecialize,
        List<ExampleMetric> sampleMetrics,
        List<String> lifecycleBreakpoints,
        List<String> booksAndRecordsImplications,
        List<String> failureModes
    ) {
    }

    public record ExampleMetric(
        String label,
        String value,
        String note
    ) {
    }
}
