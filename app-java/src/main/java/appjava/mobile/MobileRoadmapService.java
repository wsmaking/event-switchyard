package appjava.mobile;

import appjava.account.AccountOverview;
import appjava.clients.BackOfficeClient;
import appjava.clients.BackOfficeClient.BackOfficePosition;
import appjava.clients.BackOfficeClient.LedgerEntry;
import appjava.clients.OmsClient;
import appjava.market.MarketDataService;
import appjava.market.MarketStructureSnapshot;
import appjava.market.StockInfo;
import appjava.ops.ProductionEngineeringService;
import appjava.order.FillView;
import appjava.order.OrderView;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

public final class MobileRoadmapService {
    private final String accountId;
    private final MarketDataService marketDataService;
    private final BackOfficeClient backOfficeClient;
    private final OmsClient omsClient;
    private final ProductionEngineeringService productionEngineeringService;

    public MobileRoadmapService(
        String accountId,
        MarketDataService marketDataService,
        BackOfficeClient backOfficeClient,
        OmsClient omsClient,
        ProductionEngineeringService productionEngineeringService
    ) {
        this.accountId = accountId;
        this.marketDataService = marketDataService;
        this.backOfficeClient = backOfficeClient;
        this.omsClient = omsClient;
        this.productionEngineeringService = productionEngineeringService;
    }

    public InstitutionalFlowResponse buildInstitutionalFlow() {
        OrderView anchorOrder = latestOrder();
        String symbol = anchorOrder == null ? "7203" : anchorOrder.symbol();
        String symbolName = safeName(symbol);
        MarketStructureSnapshot structure = marketDataService.getMarketStructure(symbol);
        long totalQuantity = anchorOrder == null ? 12_000L : Math.max(anchorOrder.quantity() * 12L, 3_000L);
        double arrivalMid = structure.midPrice();
        double participation = Math.min(18.0, Math.max(6.0, (double) totalQuantity / Math.max(1L, structure.askQuantity() + structure.bidQuantity()) * 1.2));
        long firstSliceQty = Math.max(100L, Math.round(totalQuantity * 0.18));
        long secondSliceQty = Math.max(100L, Math.round(totalQuantity * 0.24));
        long thirdSliceQty = Math.max(100L, Math.round(totalQuantity * 0.30));
        long remainingQty = Math.max(100L, totalQuantity - firstSliceQty - secondSliceQty - thirdSliceQty);
        String clientIntent = "出来高に対して急ぎすぎず、表示価格の外へ大きく飛ばさずに " + symbolName + " を集めたい。";

        return new InstitutionalFlowResponse(
            System.currentTimeMillis(),
            anchorOrder == null ? null : anchorOrder.id(),
            symbol,
            symbolName,
            clientIntent,
            List.of(
                new ExecutionStyle(
                    "DMA",
                    "板を見ながら発注者が直接価格を決めたい場面",
                    "手数は少ないが、queue priority と self-imposed discipline が必要",
                    "clientOrderId / venueOrderId を強く意識し、cancel-replace を短く回す必要がある",
                    List.of("柔軟だが人間の判断負荷が高い", "市場状態が悪いと説明責任が個人に寄りやすい")
                ),
                new ExecutionStyle(
                    "Care Order",
                    "sales-trader が流動性探索と顧客説明を両立したい場面",
                    "価格保護とコミュニケーションを重視し、板外の流動性も探る",
                    "親注文と子注文の状態を分け、operator 向けに判断理由を残す必要がある",
                    List.of("丁寧だが自動化しすぎると文脈が消える", "裁量が増える分だけ記録が重要")
                ),
                new ExecutionStyle(
                    "POV / Participation",
                    "出来高に対する参加率を守りながら執行したい場面",
                    "市場出来高に追随し、極端な footprint を避ける",
                    "親注文 quantity と参加率、child schedule、arrival benchmark を同時に持つ必要がある",
                    List.of("相場急変で終日執行未了になり得る", "平時は説明しやすいが板が薄いと実行数量が縮む")
                )
            ),
            new ParentOrderPlan(
                anchorOrder == null ? "BUY" : anchorOrder.side(),
                totalQuantity,
                arrivalMid,
                participation,
                45,
                "POV / Participation",
                List.of(
                    "板が薄く spread が広い局面なので一撃の market order は避ける",
                    "DMA 単独では説明責任が発注者に寄りすぎる",
                    "Care order ほど人手前提にせず、child order と arrival benchmark で説明可能性を残す"
                )
            ),
            List.of(
                new ChildOrderSlice("slice-01", "opening-liquidity", firstSliceQty, arrivalMid, round2(structure.askPrice()), round2(((structure.askPrice() - arrivalMid) / Math.max(1.0, arrivalMid)) * 10_000.0), "前場寄り", "top-of-book 内で収まる数量に抑え、価格を飛ばさない"),
                new ChildOrderSlice("slice-02", "queue-build", secondSliceQty, arrivalMid, round2(arrivalMid + Math.max(1.0, structure.spread() / 2.0)), round2((((arrivalMid + Math.max(1.0, structure.spread() / 2.0)) - arrivalMid) / Math.max(1.0, arrivalMid)) * 10_000.0), "前場中盤", "bid に並んで queue priority を取りにいく"),
                new ChildOrderSlice("slice-03", "liquidity-sweep", thirdSliceQty, arrivalMid, round2(structure.askPrice() + structure.spread()), round2((((structure.askPrice() + structure.spread()) - arrivalMid) / Math.max(1.0, arrivalMid)) * 10_000.0), "後場寄り", "出来高が戻る時間帯だけ agressive に取りにいく"),
                new ChildOrderSlice("slice-04", "close-risk-control", remainingQty, arrivalMid, round2(arrivalMid + Math.max(1.0, structure.spread() * 0.7)), round2((((arrivalMid + Math.max(1.0, structure.spread() * 0.7)) - arrivalMid) / Math.max(1.0, arrivalMid)) * 10_000.0), "大引け前", "未約定残を抱えすぎず、close に向けて participation をやや上げる")
            ),
            new AllocationPlan(
                "block-equities-book",
                round2(arrivalMid + (structure.spread() * 0.55)),
                totalQuantity,
                List.of(
                    new AllocationSlice("long-only-japan", Math.round(totalQuantity * 0.50), 50.0, "benchmark 追随、tracking error 最小化が優先"),
                    new AllocationSlice("event-driven", Math.round(totalQuantity * 0.30), 30.0, "材料イベント前で早めの確保を優先"),
                    new AllocationSlice("multi-strat", totalQuantity - Math.round(totalQuantity * 0.50) - Math.round(totalQuantity * 0.30), 20.0, "残りは流動性と inventory に応じて配賦")
                ),
                "平均約定単価で book へ配賦し、child fill 単価そのものではなく block average で説明する。",
                List.of(
                    "allocation は fill 後に行い、親注文 working 中に cash を先に book へ確定しない",
                    "block quantity と配賦 quantity の総和を常に一致させる",
                    "care order と DMA を混在させる場合も allocation の truth は親注文 fill 総数に置く"
                )
            ),
            List.of(
                "arrival benchmark を親注文単位で固定しているか",
                "participation rate を market volume の変化に応じて上下させた理由を説明できるか",
                "child order ごとの aggressiveness を市場状態と結びつけて話せるか",
                "allocation が book 事情ではなく fill 実績に基づいているか"
            ),
            List.of(
                new MobileLearningService.ImplementationAnchor(
                    "注文 submit と benchmark 保存",
                    "/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/order/ExecutionBenchmarkStore.java",
                    "親注文の arrival benchmark を orderId 単位で保存して執行品質を説明する基点",
                    null
                ),
                new MobileLearningService.ImplementationAnchor(
                    "final-out と fills の集約",
                    "/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/http/OrderApiHandler.java",
                    "約定結果と execution quality を束ねて parent / child の説明に繋げる入口",
                    null
                ),
                new MobileLearningService.ImplementationAnchor(
                    "市場構造の生成",
                    "/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/market/MarketDataService.java",
                    "book depth、spread、venue state を child order 判断の前提として返す",
                    null
                )
            )
        );
    }

    public PostTradeGuideResponse buildPostTradeGuide() {
        OrderView anchorOrder = latestOrder();
        String orderId = anchorOrder == null ? null : anchorOrder.id();
        List<FillView> fills = orderId == null ? List.of() : backOfficeClient.fetchFills(orderId);
        List<LedgerEntry> ledgerEntries = orderId == null ? List.of() : backOfficeClient.fetchLedger(accountId, orderId, 20);
        AccountOverview overview = backOfficeClient.fetchOverview(accountId);
        long grossNotional = fills.stream().mapToLong(FillView::notional).sum();
        long commission = Math.max(80L, Math.round(grossNotional * 0.00025));
        long exchangeFee = Math.max(20L, Math.round(grossNotional * 0.00005));
        long taxes = "BUY".equalsIgnoreCase(anchorOrder == null ? "BUY" : anchorOrder.side()) ? 0L : Math.max(10L, Math.round(grossNotional * 0.00015));
        long netCashMovement = fills.stream().mapToLong(fill -> "BUY".equalsIgnoreCase(fill.side()) ? -fill.notional() : fill.notional()).sum()
            - commission
            - exchangeFee
            - taxes;
        long settlementBase = fills.isEmpty() ? System.currentTimeMillis() : fills.getLast().filledAt();

        return new PostTradeGuideResponse(
            System.currentTimeMillis(),
            orderId,
            anchorOrder == null ? null : anchorOrder.symbol(),
            anchorOrder == null ? "NO_ORDER" : anchorOrder.status().name(),
            List.of(
                new PostTradeStage("Execution", "front office", "約定数量と平均約定単価を確定し、parent fill と child fill を揃える", fills.isEmpty() ? "未約定" : fills.size() + " fill / 平均 " + averagePrice(fills), fills.isEmpty() ? "約定が無ければ post-trade は始まらない" : "fill が post-trade の起点"),
                new PostTradeStage("Allocation", "middle office", "book / fund へ数量と平均単価を配賦する", "3 book 配賦 / average price allocation", "親注文 fill 総数と配賦総数の一致確認"),
                new PostTradeStage("Clearing", "clearing", "約定内容を清算機関のルールに合わせて正規化する", "side / quantity / price / trade date を整形", "client-facing order view とは別に clearing instruction を持つ"),
                new PostTradeStage("Settlement", "back office", "cash と securities を受け渡し、fail を監視する", "T+2 前提 / cash move " + formatSignedYen(netCashMovement), "約定日と受渡日の区別を明示する"),
                new PostTradeStage("Books and Records", "finance & control", "ledger、statement、confirm の三系統で説明可能性を閉じる", "ledger " + ledgerEntries.size() + " 件 / cash " + formatSignedYen(overview.cashBalance()), "operator action と raw event ref を辿れるようにする")
            ),
            new FeeBreakdown(
                grossNotional,
                commission,
                exchangeFee,
                taxes,
                netCashMovement,
                List.of(
                    "commission は broker fee の教育用近似",
                    "exchange fee は venue fee / rebate を単純化",
                    "tax は side と市場ごとの差を省略した説明用近似"
                )
            ),
            new StatementPreview(
                anchorOrder == null ? "acct_demo" : anchorOrder.accountId(),
                anchorOrder == null ? "7203" : anchorOrder.symbol(),
                safeName(anchorOrder == null ? "7203" : anchorOrder.symbol()),
                fills.stream().mapToLong(FillView::quantity).sum(),
                averagePrice(fills),
                settlementLabel(settlementBase, 2),
                formatSignedYen(netCashMovement),
                List.of(
                    "注文は " + (anchorOrder == null ? "N/A" : anchorOrder.status().name()) + " で終了",
                    "statement では average price と settlement date を一行で説明する",
                    "confirm は UI の final-out より短く、法定表示の骨格を優先する"
                )
            ),
            List.of(
                new SettlementCheck("trade date と settlement date の区別", "T+2 の説明が trade timestamp と混ざっていないか", settlementLabel(settlementBase, 2)),
                new SettlementCheck("fees / taxes の控除", "gross notional と net cash movement の差が説明できるか", formatSignedYen(-(commission + exchangeFee + taxes))),
                new SettlementCheck("ledger truth", "fill 件数と ledger entry 件数に矛盾が無いか", ledgerEntries.size() + " entries")
            ),
            List.of(
                new CorporateActionHook("Dividend", "権利落ちで avg price と unrealized PnL の説明が変わる", "portfolio valuation と statement 文面の両方に影響"),
                new CorporateActionHook("Split / Reverse Split", "quantity と average price の変換が必要", "order history は変えず position 表示を調整"),
                new CorporateActionHook("Ticker Change / Merger", "symbol identity が変わっても ledger と statement の連続性を保つ", "asset master と reporting label の分離が必要")
            ),
            List.of(
                new MobileLearningService.ImplementationAnchor(
                    "fills の取得",
                    "/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/clients/BackOfficeClient.java",
                    "orderId 単位で fill と ledger を引き、post-trade 説明の根拠を作る",
                    null
                ),
                new MobileLearningService.ImplementationAnchor(
                    "ledger の確定",
                    "/Users/fujii/Desktop/dev/event-switchyard/backoffice-java/src/main/java/backofficejava/ledger",
                    "fill 起点で cash / position / realized PnL を組み立てる正本",
                    null
                ),
                new MobileLearningService.ImplementationAnchor(
                    "final-out との接続",
                    "/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/http/OrderApiHandler.java",
                    "front-to-back をまとめて照会する入口。statement と違い、raw event まで含めて説明する",
                    null
                )
            )
        );
    }

    public RiskDeepDiveResponse buildRiskDeepDive() {
        List<BackOfficePosition> positions = backOfficeClient.fetchPositions(accountId);
        AccountOverview overview = backOfficeClient.fetchOverview(accountId);
        double portfolioMarketValue = positions.stream()
            .mapToDouble(position -> marketDataService.getCurrentPrice(position.symbol()) * position.netQty())
            .sum();
        List<ConcentrationMetric> concentrations = positions.stream()
            .filter(position -> position.netQty() != 0L)
            .map(position -> {
                double currentPrice = marketDataService.getCurrentPrice(position.symbol());
                double exposure = currentPrice * position.netQty();
                double weight = portfolioMarketValue == 0.0 ? 0.0 : (exposure / portfolioMarketValue) * 100.0;
                String note = Math.abs(weight) >= 45.0
                    ? "single-name 依存が強く、stress と liquidity を同時に見る領域"
                    : Math.abs(weight) >= 25.0
                    ? "portfolio の主因。ヘッジや削減の優先候補"
                    : "補助的な寄与。単体でなく相関で読む";
                return new ConcentrationMetric(position.symbol(), safeName(position.symbol()), round2(exposure), round2(weight), note);
            })
            .sorted(Comparator.comparingDouble((ConcentrationMetric metric) -> Math.abs(metric.weightPercent())).reversed())
            .toList();

        List<LiquidityMetric> liquidity = positions.stream()
            .filter(position -> position.netQty() != 0L)
            .map(position -> {
                MarketStructureSnapshot structure = marketDataService.getMarketStructure(position.symbol());
                long visibleTop = structure.bidQuantity() + structure.askQuantity();
                double participation = visibleTop == 0 ? 0.0 : ((double) Math.abs(position.netQty()) / visibleTop) * 100.0;
                double daysToExit = visibleTop == 0 ? 0.0 : Math.max(0.5, Math.abs(position.netQty()) / Math.max(visibleTop * 6.0, 1.0));
                String note = participation > 150.0
                    ? "top-of-book に対して大きく、複数セッションに分ける前提"
                    : participation > 60.0
                    ? "POV や care order が欲しい水準"
                    : "通常の participation 範囲で説明しやすい";
                return new LiquidityMetric(position.symbol(), safeName(position.symbol()), position.netQty(), visibleTop, round2(participation), round2(daysToExit), note);
            })
            .sorted(Comparator.comparingDouble(LiquidityMetric::participationPercent).reversed())
            .toList();

        List<ScenarioLibraryEntry> scenarios = List.of(
            new ScenarioLibraryEntry("single-name-gap", "単一銘柄ギャップダウン", "concentration", "-12%", "材料イベントで one-name の risk が顕在化", "single-name exposure と liquidity を同時に確認"),
            new ScenarioLibraryEntry("market-beta-down", "市場全体の beta shock", "portfolio", "-5%", "beta の高い book で同方向リスクが顕在化", "book 全体の gross / net を説明"),
            new ScenarioLibraryEntry("spread-widening", "流動性悪化と spread 拡大", "liquidity", "+40% spread", "価格そのものより execution cost が悪化", "arrival benchmark と fill quality を再評価"),
            new ScenarioLibraryEntry("basis-break", "ヘッジ不一致", "hedge", "相関崩れ", "ヘッジ対象と実ポジションの基礎関係が崩れる", "hedge ratio だけで安心しない")
        );

        List<BacktestSample> backtestSamples = buildBacktestSamples(positions);
        long breachCount = backtestSamples.stream().filter(BacktestSample::breached).count();
        double breachRate = backtestSamples.isEmpty() ? 0.0 : (breachCount * 100.0) / backtestSamples.size();
        double averageTailLoss = backtestSamples.stream()
            .mapToDouble(BacktestSample::pnl)
            .filter(value -> value < 0.0)
            .map(Math::abs)
            .average()
            .orElse(0.0);

        return new RiskDeepDiveResponse(
            System.currentTimeMillis(),
            accountId,
            round2(portfolioMarketValue),
            round2(overview.cashBalance()),
            concentrations,
            liquidity,
            scenarios,
            new BacktestingPreview(
                backtestSamples.size(),
                round2(breachRate),
                round2(averageTailLoss),
                "教育用 backtest。観測窓は market data の直近 tick を使い、stress breach を単純判定している。",
                backtestSamples
            ),
            List.of(
                new ModelBoundary("Concentration", "gross / net と weight を見る入口", "current price と保有数量", "相関と beta decomposition"),
                new ModelBoundary("Liquidity", "exit difficulty を直感で掴む入口", "top-of-book と position size", "真の market impact model、queue depletion、venue fragmentation"),
                new ModelBoundary("Scenario Library", "何を shock するかを業務言語に変換", "single-name / market / spread widening", "stochastic correlation、vol surface"),
                new ModelBoundary("Backtesting", "前提が外れた回数をざっくり把握", "簡易 breach count と tail loss", "regulatory backtesting や model governance")
            ),
            List.of(
                new MobileLearningService.ImplementationAnchor(
                    "risk 画面の基礎計算",
                    "/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/mobile/MobileLearningService.java",
                    "shock / historical VaR / option evaluation を返している既存計算",
                    null
                ),
                new MobileLearningService.ImplementationAnchor(
                    "市場構造の利用",
                    "/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/market/MarketDataService.java",
                    "liquidity と spread widening を話す前提データ",
                    null
                )
            )
        );
    }

    public AssetClassGuideResponse buildAssetClassGuide() {
        return new AssetClassGuideResponse(
            System.currentTimeMillis(),
            List.of(
                new AssetClassLens(
                    "Equities",
                    "accepted -> working -> partial/fill/cancel/expire",
                    "last / bid-ask / spread / board depth",
                    "T+2 現物受渡",
                    "single-name concentration、beta、liquidity",
                    List.of("queue priority", "auction / halt", "corporate action"),
                    List.of("order identity", "audit trail", "OMS/BackOffice 分離"),
                    List.of("board depth", "settlement", "corporate action mapping")
                ),
                new AssetClassLens(
                    "Options",
                    "quote 取得 -> order -> fill -> position greek update",
                    "volatility surface、time to expiry、skew",
                    "premium と exercise / assignment の扱い",
                    "delta / gamma / vega / theta",
                    List.of("series selection", "expiry roll", "exercise cutoff"),
                    List.of("order lifecycle", "audit / explainability"),
                    List.of("valuation engine", "greeks refresh", "exercise event")
                ),
                new AssetClassLens(
                    "FX",
                    "RFQ / streaming quote -> fill -> nostro / cash ladder",
                    "spot / forward points / carry",
                    "currency pair ごとの cash movement",
                    "spot exposure、carry、basis",
                    List.of("session liquidity", "cutoff time", "settlement currency"),
                    List.of("execution intent", "audit trail"),
                    List.of("dual-currency ledger", "holiday calendar", "cutoff management")
                ),
                new AssetClassLens(
                    "Rates",
                    "order / RFQ -> fill -> accrual / curve update",
                    "yield curve、duration、convexity",
                    "coupon / accrual / settlement convention",
                    "DV01、curve shock、basis",
                    List.of("day count", "holiday convention", "curve roll"),
                    List.of("position truth", "operator action trace"),
                    List.of("valuation conventions", "curve build", "cashflow schedule")
                ),
                new AssetClassLens(
                    "Credit",
                    "axes / RFQ -> trade -> lifecycle events",
                    "spread、hazard、recovery",
                    "coupon / default event / settlement convention",
                    "spread widening、jump-to-default",
                    List.of("liquidity pockets", "reference obligation", "event handling"),
                    List.of("trade capture", "audit", "allocation"),
                    List.of("event engine", "default workflow", "valuation source hierarchy")
                ),
                new AssetClassLens(
                    "Futures",
                    "accepted -> fill -> variation margin",
                    "front / next contract curve、basis",
                    "daily settlement と margin call",
                    "basis、roll、liquidity concentration",
                    List.of("roll schedule", "session break", "variation margin"),
                    List.of("order state", "audit trail"),
                    List.of("contract calendar", "margin workflow", "roll logic")
                )
            ),
            List.of(
                "受注、状態遷移、監査の骨格は共通化できる",
                "valuation、risk driver、settlement convention は asset class ごとに専用化する",
                "共通化しすぎると金融の意味が消え、専用化しすぎると再利用性が消える"
            ),
            List.of(
                new MobileLearningService.ImplementationAnchor(
                    "市場構造と execution quality",
                    "/Users/fujii/Desktop/dev/event-switchyard/frontend/src/components/mobile/MobileMarketStructureView.tsx",
                    "equities の具体例として board / spread / slippage を見せる画面",
                    null
                ),
                new MobileLearningService.ImplementationAnchor(
                    "risk と option 学習",
                    "/Users/fujii/Desktop/dev/event-switchyard/frontend/src/components/mobile/MobileRiskView.tsx",
                    "線形商品と option の違いを同じ UI で比較する入口",
                    null
                ),
                new MobileLearningService.ImplementationAnchor(
                    "設計判断カード",
                    "/Users/fujii/Desktop/dev/event-switchyard/frontend/src/components/mobile/MobileCardsView.tsx",
                    "共通化と専用化の判断を反復する画面",
                    null
                )
            )
        );
    }

    public OperationsEngineeringResponse buildOperationsEngineering() {
        ProductionEngineeringService.ProductionEngineeringSnapshot snapshot = productionEngineeringService.buildSnapshot();
        List<String> reconcileNotes = new ArrayList<>();
        reconcileNotes.addAll(snapshot.omsProjection().notes());
        reconcileNotes.addAll(snapshot.backOfficeProjection().notes());
        List<String> activeIncidentTitles = snapshot.incidents().stream()
            .filter(incident -> !"OK".equals(incident.severity()))
            .map(incident -> incident.severity() + " " + incident.title())
            .limit(4)
            .toList();
        int activeIncidentCount = (int) snapshot.incidents().stream()
            .filter(incident -> !"OK".equals(incident.severity()))
            .count();
        String liveCompletenessLabel = activeIncidentTitles.isEmpty()
            ? "fills / reservation / ledger / reconcile OK"
            : "incident " + activeIncidentTitles.size() + " 件あり";

        return new OperationsEngineeringResponse(
            snapshot.generatedAt(),
            new LiveStateSummary(
                snapshot.gateway().state(),
                snapshot.omsProjection().state(),
                snapshot.backOfficeProjection().state(),
                snapshot.marketData().state(),
                snapshot.schema().state(),
                snapshot.capacity().state(),
                (int) (snapshot.omsProjection().sequenceGaps() + snapshot.backOfficeProjection().sequenceGaps()),
                snapshot.omsProjection().pendingOrphans() + snapshot.backOfficeProjection().pendingOrphans(),
                snapshot.omsProjection().deadLetters() + snapshot.backOfficeProjection().deadLetters(),
                activeIncidentCount,
                activeIncidentTitles.isEmpty() ? List.of("active incident なし") : activeIncidentTitles,
                reconcileNotes.isEmpty() ? List.of("reconcile issue なし") : reconcileNotes.stream().limit(6).toList()
            ),
            List.of(
                new SessionMonitor(
                    "Gateway / Venue session",
                    snapshot.gateway().state(),
                    "受注は続いても gateway queue と latency が悪化すると venue 説明と cancel 応答が一緒に崩れる。",
                    "queue=" + snapshot.gateway().queueLength() + " / p99=" + formatNs(snapshot.gateway().latencyP99Ns()),
                    List.of("health status", "queue length", "p99 latency", "venue heartbeat explanation"),
                    snapshot.gateway().notes()
                ),
                new SessionMonitor(
                    "OMS projection",
                    snapshot.omsProjection().state(),
                    "accepted / cancel / fill の順序が崩れると注文状態の説明が閉じない。",
                    "gap=" + snapshot.omsProjection().sequenceGaps()
                        + " / pending=" + snapshot.omsProjection().pendingOrphans()
                        + " / DLQ=" + snapshot.omsProjection().deadLetters(),
                    List.of("aggregate progress", "pending orphan", "DLQ", "bus pending", "last event age"),
                    snapshot.omsProjection().notes()
                ),
                new SessionMonitor(
                    "BackOffice projection",
                    snapshot.backOfficeProjection().state(),
                    "cash / position / realized PnL の説明責任を閉じる最後の壁。",
                    "gap=" + snapshot.backOfficeProjection().sequenceGaps()
                        + " / pending=" + snapshot.backOfficeProjection().pendingOrphans()
                        + " / DLQ=" + snapshot.backOfficeProjection().deadLetters(),
                    List.of("ledger truth", "reconcile issues", "bus pending", "last event age"),
                    snapshot.backOfficeProjection().notes()
                ),
                new SessionMonitor(
                    "Market data freshness",
                    snapshot.marketData().state(),
                    "価格 stale は execution quality と risk の前提を同時に壊す。",
                    "maxAge=" + snapshot.marketData().maxTickAgeMs() + "ms / stale=" + snapshot.marketData().staleSymbolCount(),
                    List.of("tick freshness", "auction / halt watch", "spread widening", "benchmark 混同防止"),
                    snapshot.marketData().notes()
                )
            ),
            snapshot.incidents().stream().map(incident -> new IncidentDrill(
                incident.title(),
                incident.summary(),
                List.of(
                    "これは session / projection / schema / feed のどれに属するか",
                    "raw event と live metric のどちらが崩れているか",
                    "ユーザへの説明責任はどこで開いたままか"
                ),
                List.of(
                    incident.firstAction(),
                    "pending orphan と DLQ を分けて数える",
                    "復旧後に reconcile と final-out completeness を再確認する"
                ),
                incident.severity(),
                !"OK".equals(incident.severity()),
                "incident が解消し state が RUNNING / COMPATIBLE / FRESH に戻る"
            )).toList(),
            List.of(
                new SchemaControl(
                    "Event contract",
                    "新規項目は optional / additive を原則とする。",
                    "old consumer が decode 不能になり dead letter が増える。",
                    snapshot.schema().state(),
                    snapshot.schema().notes()
                ),
                new SchemaControl(
                    "Replay safety",
                    "schema 変更時は replay と checkpoint 互換を先に確認する。",
                    "過去 audit が読めず recovery drill が壊れる。",
                    snapshot.omsProjection().busPending() > 0 || snapshot.backOfficeProjection().busPending() > 0 ? "WATCH" : "READY",
                    List.of(
                        "OMS bus pending=" + snapshot.omsProjection().busPending(),
                        "BackOffice bus pending=" + snapshot.backOfficeProjection().busPending(),
                        "replay 前に additive-only を崩していないか確認する"
                    )
                ),
                new SchemaControl(
                    "Operator wording",
                    "UI 文言を変えても raw event / source field は維持する。",
                    "runbook と現場画面の用語がずれ、復旧時の会話が壊れる。",
                    "REQUIRED",
                    List.of(
                        "final-out と statement の意味を分ける",
                        "accepted / filled / settlement を同義語として扱わない"
                    )
                )
            ),
            List.of(
                new CapacityControl(
                    "Order intake latency",
                    "gateway p99 latency",
                    "< 2ms warning / < 5ms critical",
                    "hot path に学習や集約ロジックを混ぜない理由。",
                    formatNs(snapshot.gateway().latencyP99Ns()),
                    snapshot.gateway().state(),
                    snapshot.gateway().notes()
                ),
                new CapacityControl(
                    "Projection backlog",
                    "pending orphan + DLQ + bus pending",
                    "0 を維持し、増加傾向なら projection 側を疑う",
                    "offset だけでは安全とは言えない。",
                    String.valueOf(snapshot.capacity().projectionBacklog()),
                    snapshot.capacity().state(),
                    snapshot.capacity().notes()
                ),
                new CapacityControl(
                    "Market data freshness budget",
                    "max tick age",
                    "< " + snapshot.marketData().freshnessBudgetMs() + "ms",
                    "execution と risk の両方の前提を守る閾値。",
                    snapshot.marketData().maxTickAgeMs() + "ms",
                    snapshot.marketData().state(),
                    snapshot.marketData().symbols().stream()
                        .limit(3)
                        .map(symbol -> symbol.symbol() + " " + symbol.tickAgeMs() + "ms / " + symbol.venueState())
                        .toList()
                ),
                new CapacityControl(
                    "UI trust signal",
                    "final-out completeness / reconcile",
                    "fills / reservation / ledger が揃ってから説明を閉じる",
                    "中途半端な画面で成功判定しない。",
                    liveCompletenessLabel,
                    snapshot.backOfficeProjection().state(),
                    List.of(
                        "reconcile issues=" + reconcileNotes.size(),
                        "active incidents=" + snapshot.incidents().size()
                    )
                )
            ),
            snapshot.operatorSequence(),
            List.of(
                new MobileLearningService.ImplementationAnchor(
                    "production engineering snapshot",
                    "/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/ops/ProductionEngineeringService.java",
                    "gateway / projection / feed / schema / capacity を live に集約する本体",
                    null
                ),
                new MobileLearningService.ImplementationAnchor(
                    "ops API",
                    "/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/http/OpsApiHandler.java",
                    "mobile と operator の両方が参照する production engineering API 入口",
                    null
                ),
                new MobileLearningService.ImplementationAnchor(
                    "market feed runtime",
                    "/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/market/MarketDataService.java",
                    "tick freshness、venue state、spread を live runtime として返す",
                    null
                )
            )
        );
    }

    private OrderView latestOrder() {
        return omsClient.fetchOrders().stream()
            .sorted(Comparator.comparingLong(OrderView::submittedAt).reversed())
            .findFirst()
            .orElse(null);
    }

    private List<BacktestSample> buildBacktestSamples(List<BackOfficePosition> positions) {
        List<BacktestSample> samples = new ArrayList<>();
        if (positions.isEmpty()) {
            return samples;
        }
        int sampleCount = 8;
        for (int index = 0; index < sampleCount; index++) {
            double pnl = 0.0;
            for (BackOfficePosition position : positions) {
                List<appjava.market.PricePoint> history = marketDataService.getPriceHistory(position.symbol(), 60);
                if (history.size() < index + 2) {
                    continue;
                }
                int currentIndex = history.size() - 1 - index;
                double previous = history.get(currentIndex - 1).price();
                double current = history.get(currentIndex).price();
                pnl += (current - previous) * position.netQty();
            }
            double rounded = round2(pnl);
            samples.add(new BacktestSample("tick-" + (sampleCount - index), rounded, rounded < -80_000.0));
        }
        return samples;
    }

    private String safeName(String symbol) {
        try {
            StockInfo stockInfo = marketDataService.getStockInfo(symbol);
            return stockInfo.name();
        } catch (Exception ignored) {
            return symbol;
        }
    }

    private static double averagePrice(List<FillView> fills) {
        long quantity = fills.stream().mapToLong(FillView::quantity).sum();
        if (quantity <= 0L) {
            return 0.0;
        }
        double weighted = fills.stream().mapToDouble(fill -> fill.price() * fill.quantity()).sum();
        return round2(weighted / quantity);
    }

    private static String settlementLabel(long baseTimeMillis, int plusBusinessDays) {
        return "T+" + plusBusinessDays + " 想定 (" + new java.util.Date(baseTimeMillis + (plusBusinessDays * 24L * 60L * 60L * 1000L)).toString() + ")";
    }

    private static String formatSignedYen(long value) {
        String prefix = value > 0 ? "+" : "";
        return prefix + String.format(Locale.US, "%,d円", value);
    }

    private static String safeState(String value) {
        return value == null || value.isBlank() ? "UNKNOWN" : value;
    }

    private static String formatNs(long nanos) {
        if (nanos < 0) {
            return "n/a";
        }
        if (nanos >= 1_000_000L) {
            return round2(nanos / 1_000_000.0) + "ms";
        }
        return round2(nanos / 1_000.0) + "us";
    }

    private static double round2(double value) {
        return Math.round(value * 100.0) / 100.0;
    }

    public record InstitutionalFlowResponse(
        long generatedAt,
        String anchorOrderId,
        String symbol,
        String symbolName,
        String clientIntent,
        List<ExecutionStyle> executionStyles,
        ParentOrderPlan parentOrderPlan,
        List<ChildOrderSlice> childOrders,
        AllocationPlan allocationPlan,
        List<String> operatorChecks,
        List<MobileLearningService.ImplementationAnchor> implementationAnchors
    ) {
    }

    public record ExecutionStyle(
        String name,
        String useCase,
        String businessRule,
        String systemImplication,
        List<String> tradeoffs
    ) {
    }

    public record ParentOrderPlan(
        String side,
        long totalQuantity,
        double arrivalMidPrice,
        double targetParticipationPercent,
        int scheduleWindowMinutes,
        String chosenStyle,
        List<String> whyNotOtherChoices
    ) {
    }

    public record ChildOrderSlice(
        String id,
        String venueIntent,
        long plannedQuantity,
        double benchmarkPrice,
        double expectedFillPrice,
        double expectedSlippageBps,
        String timeBucketLabel,
        String learningPoint
    ) {
    }

    public record AllocationPlan(
        String blockBook,
        double averagePrice,
        long totalQuantity,
        List<AllocationSlice> allocations,
        String settlementNote,
        List<String> controlChecks
    ) {
    }

    public record AllocationSlice(
        String targetBook,
        long quantity,
        double ratioPercent,
        String note
    ) {
    }

    public record PostTradeGuideResponse(
        long generatedAt,
        String orderId,
        String symbol,
        String orderStatus,
        List<PostTradeStage> stages,
        FeeBreakdown feeBreakdown,
        StatementPreview statementPreview,
        List<SettlementCheck> settlementChecks,
        List<CorporateActionHook> corporateActionHooks,
        List<MobileLearningService.ImplementationAnchor> implementationAnchors
    ) {
    }

    public record PostTradeStage(
        String name,
        String owner,
        String purpose,
        String currentView,
        String whyItMatters
    ) {
    }

    public record FeeBreakdown(
        long grossNotional,
        long commission,
        long exchangeFee,
        long taxes,
        long netCashMovement,
        List<String> assumptions
    ) {
    }

    public record StatementPreview(
        String accountId,
        String symbol,
        String symbolName,
        long settledQuantity,
        double averagePrice,
        String settlementDateLabel,
        String netCashMovementLabel,
        List<String> notes
    ) {
    }

    public record SettlementCheck(
        String title,
        String rule,
        String currentValue
    ) {
    }

    public record CorporateActionHook(
        String name,
        String businessImpact,
        String systemImpact
    ) {
    }

    public record RiskDeepDiveResponse(
        long generatedAt,
        String accountId,
        double marketValue,
        double cashBalance,
        List<ConcentrationMetric> concentration,
        List<LiquidityMetric> liquidity,
        List<ScenarioLibraryEntry> scenarioLibrary,
        BacktestingPreview backtesting,
        List<ModelBoundary> modelBoundaries,
        List<MobileLearningService.ImplementationAnchor> implementationAnchors
    ) {
    }

    public record ConcentrationMetric(
        String symbol,
        String symbolName,
        double exposure,
        double weightPercent,
        String note
    ) {
    }

    public record LiquidityMetric(
        String symbol,
        String symbolName,
        long positionQuantity,
        long visibleTopOfBookQuantity,
        double participationPercent,
        double estimatedDaysToExit,
        String note
    ) {
    }

    public record ScenarioLibraryEntry(
        String id,
        String title,
        String category,
        String shock,
        String rationale,
        String focus
    ) {
    }

    public record BacktestingPreview(
        int observationCount,
        double breachRatePercent,
        double averageTailLoss,
        String note,
        List<BacktestSample> samples
    ) {
    }

    public record BacktestSample(
        String label,
        double pnl,
        boolean breached
    ) {
    }

    public record ModelBoundary(
        String title,
        String whyItMatters,
        String whatIncluded,
        String whatExcluded
    ) {
    }

    public record AssetClassGuideResponse(
        long generatedAt,
        List<AssetClassLens> assetClasses,
        List<String> boundaryPrinciples,
        List<MobileLearningService.ImplementationAnchor> implementationAnchors
    ) {
    }

    public record AssetClassLens(
        String assetClass,
        String lifecycle,
        String valuationDriver,
        String settlementModel,
        String riskDriver,
        List<String> operatorWatchpoints,
        List<String> whatStaysCommon,
        List<String> whatMustSpecialize
    ) {
    }

    public record OperationsEngineeringResponse(
        long generatedAt,
        LiveStateSummary liveState,
        List<SessionMonitor> sessionMonitors,
        List<IncidentDrill> incidentDrills,
        List<SchemaControl> schemaControls,
        List<CapacityControl> capacityControls,
        List<String> operatorSequence,
        List<MobileLearningService.ImplementationAnchor> implementationAnchors
    ) {
    }

    public record LiveStateSummary(
        String gatewayState,
        String omsState,
        String backOfficeState,
        String marketDataState,
        String schemaState,
        String capacityState,
        int sequenceGapCount,
        int pendingOrphanCount,
        int deadLetterCount,
        int activeIncidentCount,
        List<String> activeIncidents,
        List<String> reconcileNotes
    ) {
    }

    public record SessionMonitor(
        String name,
        String state,
        String whyItMatters,
        String currentValue,
        List<String> checkpoints,
        List<String> operatorActions
    ) {
    }

    public record IncidentDrill(
        String name,
        String trigger,
        List<String> firstQuestions,
        List<String> actions,
        String severity,
        boolean active,
        String recoverySignal
    ) {
    }

    public record SchemaControl(
        String title,
        String rule,
        String failureMode,
        String currentState,
        List<String> operatorChecks
    ) {
    }

    public record CapacityControl(
        String title,
        String metric,
        String threshold,
        String whyItMatters,
        String currentValue,
        String status,
        List<String> operatorActions
    ) {
    }
}
