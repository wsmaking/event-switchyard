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
        List<String> booksAndRecordsImplications,
        List<String> failureModes
    ) {
    }
}
