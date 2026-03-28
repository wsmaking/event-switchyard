package appjava.mobile;

import appjava.account.AccountOverview;
import appjava.clients.BackOfficeClient;
import appjava.clients.BackOfficeClient.BackOfficePosition;
import appjava.clients.BackOfficeClient.BackOfficeReconcile;
import appjava.clients.BackOfficeClient.BackOfficeStats;
import appjava.clients.OmsClient;
import appjava.clients.OmsClient.OmsReconcile;
import appjava.clients.OmsClient.OmsStats;
import appjava.market.MarketDataService;
import appjava.market.PricePoint;
import appjava.market.StockInfo;
import appjava.order.OrderView;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

public final class MobileLearningService {
    private final String accountId;
    private final MarketDataService marketDataService;
    private final BackOfficeClient backOfficeClient;
    private final OmsClient omsClient;
    private final MobileProgressStore progressStore;
    private final List<LearningCard> cards;
    private final Map<String, LearningCard> cardIndex;
    private final List<RiskScenario> riskScenarios;

    public MobileLearningService(
        String accountId,
        MarketDataService marketDataService,
        BackOfficeClient backOfficeClient,
        OmsClient omsClient,
        MobileProgressStore progressStore
    ) {
        this.accountId = accountId;
        this.marketDataService = marketDataService;
        this.backOfficeClient = backOfficeClient;
        this.omsClient = omsClient;
        this.progressStore = progressStore;
        this.cards = buildCards();
        this.cardIndex = new LinkedHashMap<>();
        this.cards.forEach(card -> cardIndex.put(card.id(), card));
        this.riskScenarios = buildRiskScenarios();
    }

    public MobileHomeResponse buildHome() {
        List<OrderView> recentOrders = omsClient.fetchOrders().stream()
            .sorted(Comparator.comparingLong(OrderView::submittedAt).reversed())
            .limit(6)
            .toList();
        MobileProgressStore.ProgressSnapshot progress = progressStore.snapshot();
        List<CardSummary> cardSummaries = summarizeCards(progress);
        List<CardSummary> dueCards = cardSummaries.stream()
            .filter(CardSummary::due)
            .limit(4)
            .toList();
        List<CardSummary> bookmarks = cardSummaries.stream()
            .filter(CardSummary::bookmarked)
            .limit(4)
            .toList();
        String anchorRoute = progress.anchor() != null && progress.anchor().route() != null
            ? progress.anchor().route()
            : "/mobile/orders";
        String anchorOrderId = progress.anchor() == null ? null : progress.anchor().orderId();
        String anchorCardId = progress.anchor() == null ? null : progress.anchor().cardId();
        String continueRoute = anchorRoute;
        if ("/mobile".equals(continueRoute) || continueRoute.isBlank()) {
            continueRoute = recentOrders.isEmpty() ? "/mobile/cards" : "/mobile/orders/" + recentOrders.getFirst().id();
        }

        OmsStats omsStats = omsClient.fetchStats();
        BackOfficeStats backOfficeStats = backOfficeClient.fetchStats();
        OmsReconcile omsReconcile = omsClient.fetchReconcile(accountId);
        BackOfficeReconcile backOfficeReconcile = backOfficeClient.fetchReconcile(accountId);
        MainlineStatus mainlineStatus = buildMainlineStatus(omsStats, backOfficeStats, omsReconcile, backOfficeReconcile);

        String scenarioRoute = recentOrders.isEmpty() ? "/mobile/cards" : "/mobile/orders/" + recentOrders.getFirst().id();
        String scenarioTitle = recentOrders.isEmpty() ? "設計カード 5分復習" : "最近の注文を追う";
        String scenarioBody = recentOrders.isEmpty()
            ? "outbox / audit / sequence の判断を短時間で反復"
            : recentOrders.getFirst().symbol() + " の final-out から reservation と台帳を追跡";

        return new MobileHomeResponse(
            accountId,
            System.currentTimeMillis(),
            new ContinueLearning(
                continueRoute,
                anchorOrderId,
                anchorCardId,
                recentOrders.isEmpty() ? "設計判断から再開" : "最近の注文から再開",
                recentOrders.isEmpty() ? "カードで設計意図を再確認" : "timeline と final-out を追跡"
            ),
            new StudySuggestion(
                "今日の 5 分テーマ",
                scenarioTitle,
                scenarioBody,
                scenarioRoute
            ),
            mainlineStatus,
            recentOrders.stream().map(this::toOrderDigest).toList(),
            dueCards,
            bookmarks,
            List.of(
                new QuickAction("注文フローを見る", recentOrders.isEmpty() ? "/mobile/orders" : "/mobile/orders/" + recentOrders.getFirst().id(), "注文"),
                new QuickAction("台帳フローを見る", "/mobile/ledger", "台帳"),
                new QuickAction("障害導線を見る", "/mobile/architecture", "運用"),
                new QuickAction("設計カードに入る", "/mobile/cards", "設計"),
                new QuickAction("risk sandbox を開く", "/mobile/risk", "リスク")
            ),
            progressSummary(progress, cardSummaries)
        );
    }

    public List<CardSummary> listCards() {
        return summarizeCards(progressStore.snapshot());
    }

    public CardDetail getCard(String cardId) {
        LearningCard card = Optional.ofNullable(cardIndex.get(cardId))
            .orElseThrow(() -> new IllegalArgumentException("card_not_found:" + cardId));
        MobileProgressStore.CardProgress progress = progressStore.snapshot().cards().get(cardId);
        return new CardDetail(card, toCardProgress(progress, card));
    }

    public ProgressResponse getProgress() {
        MobileProgressStore.ProgressSnapshot progress = progressStore.snapshot();
        List<CardSummary> cards = summarizeCards(progress);
        return new ProgressResponse(
            progress.accountId(),
            progress.updatedAt(),
            progress.anchor(),
            (int) cards.stream().filter(CardSummary::due).count(),
            (int) cards.stream().filter(CardSummary::bookmarked).count(),
            (int) cards.stream().filter(card -> card.progress().completed()).count(),
            cards
        );
    }

    public ProgressResponse applyProgress(ProgressUpdateRequest request) {
        if (request == null || request.type() == null || request.type().isBlank()) {
            throw new IllegalArgumentException("progress_type_required");
        }
        MobileProgressStore.ProgressSnapshot snapshot = switch (request.type()) {
            case "anchor" -> progressStore.applyAnchor(
                valueOrDefault(request.route(), "/mobile"),
                blankToNull(request.orderId()),
                blankToNull(request.cardId())
            );
            case "bookmark" -> {
                requireCardId(request.cardId());
                yield progressStore.setBookmark(request.cardId(), Boolean.TRUE.equals(request.bookmarked()));
            }
            case "review" -> {
                requireCardId(request.cardId());
                yield progressStore.reviewCard(request.cardId(), Boolean.TRUE.equals(request.correct()));
            }
            default -> throw new IllegalArgumentException("unsupported_progress_type:" + request.type());
        };
        List<CardSummary> cards = summarizeCards(snapshot);
        return new ProgressResponse(
            snapshot.accountId(),
            snapshot.updatedAt(),
            snapshot.anchor(),
            (int) cards.stream().filter(CardSummary::due).count(),
            (int) cards.stream().filter(CardSummary::bookmarked).count(),
            (int) cards.stream().filter(card -> card.progress().completed()).count(),
            cards
        );
    }

    public List<RiskScenario> listRiskScenarios() {
        return riskScenarios;
    }

    public RiskEvaluationResponse evaluateRisk(RiskEvaluationRequest request) {
        RiskScenario scenario = resolveRiskScenario(request);
        List<BackOfficePosition> positions = backOfficeClient.fetchPositions(accountId);
        AccountOverview overview = backOfficeClient.fetchOverview(accountId);
        List<RiskPositionImpact> impacts = calculateImpacts(positions, scenario, null, 0.0);
        PortfolioImpact portfolioImpact = toPortfolioImpact(impacts, overview);
        HistoricalVar historicalVar = buildHistoricalVar(positions);
        HedgeComparison hedgeComparison = buildHedgeComparison(positions, scenario, portfolioImpact.pnlDelta());
        List<String> assumptions = new ArrayList<>(scenario.assumptions());
        assumptions.add("historical VaR は過去価格の 1-step return を使う教育用近似");
        assumptions.add("hedge comparison は最大エクスポージャー 50% 圧縮の簡易比較");
        return new RiskEvaluationResponse(
            accountId,
            scenario.id(),
            scenario.title(),
            scenario.description(),
            System.currentTimeMillis(),
            portfolioImpact,
            impacts,
            historicalVar,
            hedgeComparison,
            assumptions
        );
    }

    static boolean isDue(MobileProgressStore.CardProgress progress, long now) {
        if (progress == null) {
            return true;
        }
        return !progress.completed() || progress.nextReviewAt() <= now;
    }

    private MainlineStatus buildMainlineStatus(
        OmsStats omsStats,
        BackOfficeStats backOfficeStats,
        OmsReconcile omsReconcile,
        BackOfficeReconcile backOfficeReconcile
    ) {
        int sequenceGapCount = (int) ((omsStats == null ? 0 : omsStats.sequenceGaps()) + (backOfficeStats == null ? 0 : backOfficeStats.sequenceGaps()));
        int pendingOrphans = (omsStats == null ? 0 : omsStats.pendingOrphanCount()) + (backOfficeStats == null ? 0 : backOfficeStats.pendingOrphanCount());
        int deadLetters = (omsStats == null ? 0 : omsStats.deadLetterCount()) + (backOfficeStats == null ? 0 : backOfficeStats.deadLetterCount());
        List<String> notes = new ArrayList<>();
        if (omsReconcile != null && omsReconcile.issues() != null) {
            notes.addAll(omsReconcile.issues());
        }
        if (backOfficeReconcile != null && backOfficeReconcile.issues() != null) {
            notes.addAll(backOfficeReconcile.issues());
        }
        if (notes.isEmpty()) {
            notes.add("reconcile issue なし");
        }
        boolean healthy = isRunning(omsStats == null ? null : omsStats.state())
            && isRunning(backOfficeStats == null ? null : backOfficeStats.state())
            && sequenceGapCount == 0
            && pendingOrphans == 0
            && deadLetters == 0
            && notes.size() == 1
            && "reconcile issue なし".equals(notes.getFirst());

        String summary = healthy
            ? "mainline stable"
            : "確認ポイント: gap " + sequenceGapCount + " / pending " + pendingOrphans + " / dlq " + deadLetters;

        return new MainlineStatus(
            healthy,
            summary,
            omsStats == null ? "UNKNOWN" : omsStats.state(),
            backOfficeStats == null ? "UNKNOWN" : backOfficeStats.state(),
            sequenceGapCount,
            pendingOrphans,
            deadLetters,
            notes.stream().limit(4).toList()
        );
    }

    private MobileOrderDigest toOrderDigest(OrderView order) {
        String learningFocus = switch (order.status()) {
            case PARTIALLY_FILLED -> "reservation 縮小と fill の同時追跡";
            case FILLED -> "final-out で cash / position / raw events を確認";
            case REJECTED -> "拒否理由と gateway 境界を確認";
            case CANCELED, CANCEL_PENDING -> "cancel / cancel-replace の導線確認";
            case EXPIRED -> "expire と release の挙動確認";
            default -> "accepted から final-out まで追跡";
        };
        return new MobileOrderDigest(
            order.id(),
            order.symbol(),
            safeName(order.symbol()),
            order.status().name(),
            order.submittedAt(),
            order.filledQuantity(),
            order.remainingQuantity(),
            learningFocus
        );
    }

    private List<CardSummary> summarizeCards(MobileProgressStore.ProgressSnapshot progress) {
        long now = System.currentTimeMillis();
        return cards.stream()
            .map(card -> {
                MobileProgressStore.CardProgress current = progress.cards().get(card.id());
                CardProgress summary = toCardProgress(current, card);
                return new CardSummary(
                    card.id(),
                    card.title(),
                    card.category(),
                    card.difficulty(),
                    summary.bookmarked(),
                    isDue(current, now),
                    summary,
                    card.route()
                );
            })
            .toList();
    }

    private static CardProgress toCardProgress(MobileProgressStore.CardProgress progress, LearningCard card) {
        if (progress == null) {
            return new CardProgress(card.id(), false, false, 0, 0, 0, 0L, 0L);
        }
        return new CardProgress(
            progress.cardId(),
            progress.bookmarked(),
            progress.completed(),
            progress.masteryLevel(),
            progress.correctCount(),
            progress.incorrectCount(),
            progress.lastReviewedAt(),
            progress.nextReviewAt()
        );
    }

    private ProgressSummary progressSummary(MobileProgressStore.ProgressSnapshot progress, List<CardSummary> cards) {
        return new ProgressSummary(
            progress.anchor(),
            (int) cards.stream().filter(CardSummary::due).count(),
            (int) cards.stream().filter(CardSummary::bookmarked).count(),
            (int) cards.stream().filter(card -> card.progress().completed()).count()
        );
    }

    private List<RiskPositionImpact> calculateImpacts(
        List<BackOfficePosition> positions,
        RiskScenario scenario,
        String hedgeSymbol,
        double hedgeRatio
    ) {
        List<RiskPositionImpact> impacts = new ArrayList<>();
        for (BackOfficePosition position : positions) {
            double quantityFactor = hedgeSymbol != null && hedgeSymbol.equals(position.symbol())
                ? Math.max(0.0, 1.0 - hedgeRatio)
                : 1.0;
            long effectiveQty = Math.round(position.netQty() * quantityFactor);
            double currentPrice = marketDataService.getCurrentPrice(position.symbol());
            double shockPercent = scenarioShockForSymbol(scenario, position.symbol());
            double shockedPrice = round2(currentPrice * (1.0 + shockPercent / 100.0));
            double currentValue = round2(currentPrice * effectiveQty);
            double stressedValue = round2(shockedPrice * effectiveQty);
            double delta = round2(stressedValue - currentValue);
            impacts.add(new RiskPositionImpact(
                position.symbol(),
                safeName(position.symbol()),
                effectiveQty,
                round2(position.avgPrice()),
                round2(currentPrice),
                shockedPrice,
                round2(currentValue),
                round2(stressedValue),
                delta,
                shockPercent
            ));
        }
        impacts.sort(Comparator.comparingDouble((RiskPositionImpact impact) -> Math.abs(impact.pnlDelta())).reversed());
        return impacts;
    }

    private static PortfolioImpact toPortfolioImpact(List<RiskPositionImpact> impacts, AccountOverview overview) {
        double currentMarketValue = 0.0;
        double shockedMarketValue = 0.0;
        double pnlDelta = 0.0;
        for (RiskPositionImpact impact : impacts) {
            currentMarketValue += impact.currentValue();
            shockedMarketValue += impact.shockedValue();
            pnlDelta += impact.pnlDelta();
        }
        return new PortfolioImpact(
            round2(currentMarketValue),
            round2(shockedMarketValue),
            round2(pnlDelta),
            overview.cashBalance(),
            overview.realizedPnl()
        );
    }

    private HistoricalVar buildHistoricalVar(List<BackOfficePosition> positions) {
        List<List<PricePoint>> histories = new ArrayList<>();
        List<BackOfficePosition> activePositions = new ArrayList<>();
        int observationCount = Integer.MAX_VALUE;
        for (BackOfficePosition position : positions) {
            if (position.netQty() == 0L) {
                continue;
            }
            List<PricePoint> history = marketDataService.getPriceHistory(position.symbol(), 120);
            if (history.size() < 2) {
                continue;
            }
            histories.add(history);
            activePositions.add(position);
            observationCount = Math.min(observationCount, history.size() - 1);
        }
        if (activePositions.isEmpty() || observationCount <= 0) {
            return new HistoricalVar(95, 0, 0.0, 0.0, "1 tick", "insufficient_history");
        }
        List<Double> pnlSamples = new ArrayList<>();
        for (int offset = 0; offset < observationCount; offset++) {
            double pnl = 0.0;
            for (int index = 0; index < activePositions.size(); index++) {
                List<PricePoint> history = histories.get(index);
                BackOfficePosition position = activePositions.get(index);
                int startIndex = history.size() - observationCount - 1 + offset;
                double previous = history.get(startIndex).price();
                double current = history.get(startIndex + 1).price();
                pnl += (current - previous) * position.netQty();
            }
            pnlSamples.add(round2(pnl));
        }
        pnlSamples.sort(Double::compareTo);
        int tailSize = Math.max(1, (int) Math.ceil(pnlSamples.size() * 0.05));
        double varLoss = Math.abs(Math.min(0.0, pnlSamples.get(tailSize - 1)));
        double expectedShortfall = 0.0;
        for (int index = 0; index < tailSize; index++) {
            expectedShortfall += Math.abs(Math.min(0.0, pnlSamples.get(index)));
        }
        expectedShortfall = round2(expectedShortfall / tailSize);
        return new HistoricalVar(95, observationCount, round2(varLoss), expectedShortfall, "1 tick", "過去 " + observationCount + " 本の return");
    }

    private HedgeComparison buildHedgeComparison(List<BackOfficePosition> positions, RiskScenario scenario, double unhedgedPnlDelta) {
        if (positions.isEmpty()) {
            return new HedgeComparison(null, 0.0, round2(unhedgedPnlDelta), round2(unhedgedPnlDelta), 0.0, "ポジションなし");
        }
        String hedgeSymbol = scenario.targetSymbol();
        if (hedgeSymbol == null) {
            hedgeSymbol = positions.stream()
                .max(Comparator.comparingDouble(position -> Math.abs(position.netQty() * marketDataService.getCurrentPrice(position.symbol()))))
                .map(BackOfficePosition::symbol)
                .orElse(null);
        }
        if (hedgeSymbol == null) {
            return new HedgeComparison(null, 0.0, round2(unhedgedPnlDelta), round2(unhedgedPnlDelta), 0.0, "hedge symbol を決められませんでした");
        }
        double hedgeRatio = 0.5;
        List<RiskPositionImpact> hedgedImpacts = calculateImpacts(positions, scenario, hedgeSymbol, hedgeRatio);
        double hedgedPnlDelta = hedgedImpacts.stream().mapToDouble(RiskPositionImpact::pnlDelta).sum();
        double protectionAmount = round2(Math.abs(unhedgedPnlDelta) - Math.abs(hedgedPnlDelta));
        return new HedgeComparison(
            hedgeSymbol,
            hedgeRatio,
            round2(unhedgedPnlDelta),
            round2(hedgedPnlDelta),
            protectionAmount,
            hedgeSymbol + " のエクスポージャーを 50% 落とした教育用比較"
        );
    }

    private RiskScenario resolveRiskScenario(RiskEvaluationRequest request) {
        if (request != null && request.customShockPercent() != null) {
            String targetSymbol = blankToNull(request.targetSymbol());
            String title = targetSymbol == null
                ? "Custom market shock"
                : "Custom single-name shock";
            String description = targetSymbol == null
                ? "全ポジションに " + request.customShockPercent() + "% shock を適用"
                : targetSymbol + " に " + request.customShockPercent() + "% shock を適用";
            return new RiskScenario(
                "custom",
                title,
                description,
                targetSymbol,
                request.customShockPercent(),
                targetSymbol == null ? "portfolio" : "single-name",
                List.of(
                    "教育用の単純ショック",
                    "ボラ、相関、流動性、greeks は未考慮",
                    "reservation は margin ではない"
                )
            );
        }
        String scenarioId = request == null ? null : blankToNull(request.scenarioId());
        if (scenarioId == null) {
            return riskScenarios.getFirst();
        }
        return riskScenarios.stream()
            .filter(candidate -> candidate.id().equals(scenarioId))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("risk_scenario_not_found:" + scenarioId));
    }

    private double scenarioShockForSymbol(RiskScenario scenario, String symbol) {
        if ("single-name".equals(scenario.scope())) {
            return symbol.equals(scenario.targetSymbol()) ? scenario.shockPercent() : 0.0;
        }
        return scenario.shockPercent();
    }

    private static boolean isRunning(String state) {
        return state != null && ("RUNNING".equalsIgnoreCase(state) || "ENABLED".equalsIgnoreCase(state));
    }

    private static String valueOrDefault(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }

    private static void requireCardId(String cardId) {
        if (cardId == null || cardId.isBlank()) {
            throw new IllegalArgumentException("card_id_required");
        }
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private String safeName(String symbol) {
        try {
            StockInfo stockInfo = marketDataService.getStockInfo(symbol);
            return stockInfo.name();
        } catch (Exception ignored) {
            return symbol;
        }
    }

    private static double round2(double value) {
        return Math.round(value * 100.0) / 100.0;
    }

    private static List<LearningCard> buildCards() {
        List<LearningCard> built = new ArrayList<>(List.of(
            new LearningCard(
                "oms-vs-backoffice",
                "OMS と BackOffice の境界",
                "設計",
                "medium",
                "同じ注文を見ているのに、なぜ OMS と BackOffice を分けるのか。",
                "OMS は注文状態の収束、BackOffice は cash / position / P&L / ledger の正本責務。",
                "OMS では accepted / partial / filled / canceled / expired の注文ライフサイクルを管理する。BackOffice では fills 起点で cash、reservation release、position、realized P&L、ledger を確定させる。両者を分けることで、注文状態の競合と会計整合の論点を独立に扱える。",
                List.of("/mobile/orders", "/mobile/ledger"),
                List.of(
                    "/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/http/OrderApiHandler.java",
                    "/Users/fujii/Desktop/dev/event-switchyard/backoffice-java/src/main/java/backofficejava/http/LedgerHttpHandler.java"
                ),
                List.of("注文状態管理", "台帳正本", "責務分離")
            ),
            new LearningCard(
                "reservation-vs-margin",
                "reservation と margin の違い",
                "リスク",
                "medium",
                "reservation をそのまま margin と呼ばない理由は何か。",
                "reservation は注文拘束。margin はポートフォリオ全体のリスク前提を使う別物。",
                "この repo の reservation は OMS が注文受理時に買付余力を拘束する仕組みであり、portfolio margin のように相関やボラを用いた証拠金計算ではない。面接では、reservation は operational control、margin は risk model と切り分けて話す必要がある。",
                List.of("/mobile/ledger", "/mobile/risk"),
                List.of(
                    "/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/demo/ReplayScenarioService.java",
                    "/Users/fujii/Desktop/dev/event-switchyard/docs/ops/mobile_risk_learning_platform_plan.md"
                ),
                List.of("reservation", "margin", "risk model")
            ),
            new LearningCard(
                "outbox-audit-bus",
                "outbox / audit / bus を分ける理由",
                "設計",
                "hard",
                "なぜ single event stream ではなく、outbox、audit、bus を分けるのか。",
                "再送、観測、下流連携の責務が違うから。",
                "outbox は gateway 内の durable emission 境界、audit は復元と説明責務、bus は下流 consumer への配信責務を持つ。1 本にまとめると、fast path の責務と downstream reliability の責務が絡み、再送戦略や運用説明が曖昧になる。",
                List.of("/mobile/architecture"),
                List.of(
                    "/Users/fujii/Desktop/dev/event-switchyard/gateway-rust/src/outbox",
                    "/Users/fujii/Desktop/dev/event-switchyard/docs/ops/business_mainline_operations_runbook.md"
                ),
                List.of("outbox", "audit", "bus")
            ),
            new LearningCard(
                "aggregate-seq",
                "aggregateSeq gap を保留する理由",
                "運用",
                "hard",
                "aggregateSeq を見ずに届いた順に apply すると何が壊れるか。",
                "cancel / fill / reservation release の順序が崩れ、projection が壊れる。",
                "Java 側 projection は aggregateSeq を使って gap を pending orphan として保留する。これが無いと partial fill より前に cancel complete を適用する、accepted 前に fill を会計反映する、といった順序破綻が起きる。",
                List.of("/mobile/architecture"),
                List.of(
                    "/Users/fujii/Desktop/dev/event-switchyard/oms-java/src/main/java/oms/audit/GatewayAuditIntakeService.java",
                    "/Users/fujii/Desktop/dev/event-switchyard/backoffice-java/src/main/java/backofficejava/audit/GatewayAuditIntakeService.java"
                ),
                List.of("aggregateSeq", "pending orphan", "projection")
            ),
            new LearningCard(
                "cancel-replace",
                "amend を cancel-replace で扱う理由",
                "注文",
                "medium",
                "なぜ amend を単純 update せず cancel-replace に寄せるのか。",
                "venue 制約と監査説明が明確になるから。",
                "多くの venue では amend 可能項目が限られ、内部的には cancel と new order の連鎖として扱った方が説明と整合が取りやすい。この repo でも amend は cancel-confirm 後の replace に寄せている。",
                List.of("/mobile/orders", "/mobile/architecture"),
                List.of(
                    "/Users/fujii/Desktop/dev/event-switchyard/gateway-rust/src/server/http/orders/classic.rs",
                    "/Users/fujii/Desktop/dev/event-switchyard/gateway-rust/src/exchange/control.rs"
                ),
                List.of("cancel-replace", "venue control")
            ),
            new LearningCard(
                "reconcile-purpose",
                "reconcile の目的",
                "運用",
                "medium",
                "reconcile は何のためにあり、どこで見るべきか。",
                "projection が truth からズレていないかを定期的に確認するため。",
                "orders / reservations は OMS 側、ledger / cash / positions は BackOffice 側で reconcile を持つ。数字だけでなく、issue が 0 である理由と、issue 発生時に orphan / DLQ / replay へどう繋ぐかを説明できる必要がある。",
                List.of("/mobile/architecture", "/mobile/ledger"),
                List.of(
                    "/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/http/OpsApiHandler.java",
                    "/Users/fujii/Desktop/dev/event-switchyard/docs/ops/business_mainline_operations_runbook.md"
                ),
                List.of("reconcile", "ops")
            ),
            new LearningCard(
                "hot-path-boundary",
                "gateway-rust hot path を壊さない境界",
                "設計",
                "hard",
                "なぜ学習系 UI や業務参照ロジックを gateway 側に寄せないのか。",
                "accept hot path を重くしないため。",
                "gateway-rust は受理と venue 制御の hot path を担う。mobile 学習機能や aggregation は app-java に閉じ、OMS / BackOffice で projection することで、latency と retry 論点を切り分ける。",
                List.of("/mobile/architecture"),
                List.of(
                    "/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/http/AppHttpServer.java",
                    "/Users/fujii/Desktop/dev/event-switchyard/gateway-rust/src/main.rs"
                ),
                List.of("hot path", "boundary")
            ),
            new LearningCard(
                "final-out-reading",
                "final-out の読み方",
                "注文",
                "easy",
                "final-out を見たとき、何から読むべきか。",
                "status と timeline、その次に reservation / fills / balance effect。",
                "まず order status と timeline で注文の物語を掴む。その後 reservation と fills を見て、最後に balance delta と positions で業務結果を確認する。raw events は説明の裏取りに使う。",
                List.of("/mobile/orders", "/mobile/ledger"),
                List.of(
                    "/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/http/OrderApiHandler.java"
                ),
                List.of("final-out", "timeline")
            ),
            new LearningCard(
                "dlq-vs-pending",
                "pending orphan と DLQ の違い",
                "運用",
                "medium",
                "pending orphan と DLQ はどう違うか。",
                "pending orphan は待てば解ける順序問題、DLQ は手当が必要な失敗イベント。",
                "aggregateSeq gap や accepted 未着の fill は pending orphan で保留し、前提イベント到着後に replay する。一方、解釈不能 payload や不整合は DLQ に落として operator が再投入や調査を行う。",
                List.of("/mobile/architecture"),
                List.of(
                    "/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/http/OpsApiHandler.java",
                    "/Users/fujii/Desktop/dev/event-switchyard/docs/ops/business_mainline_operations_runbook.md"
                ),
                List.of("pending orphan", "DLQ")
            ),
            new LearningCard(
                "risk-sandbox-limit",
                "教育用 risk sandbox の限界",
                "リスク",
                "easy",
                "この mobile risk sandbox の数字を本番 risk と同一視してはいけない理由は何か。",
                "相関、流動性、ボラ、保有期間、信頼水準を省略しているから。",
                "この risk sandbox はポジションと現在価格に shock をかける教育用の簡易モデルであり、VaR engine ではない。面接では、数字よりも前提と限界を説明できることが重要。",
                List.of("/mobile/risk"),
                List.of(
                    "/Users/fujii/Desktop/dev/event-switchyard/docs/ops/mobile_risk_learning_platform_plan.md"
                ),
                List.of("risk sandbox", "assumption")
            )
        ));
        built.addAll(buildExtendedCards());
        return List.copyOf(built);
    }

    private static List<LearningCard> buildExtendedCards() {
        return List.of(
            new LearningCard(
                "idempotency-boundary",
                "idempotency key をどこで効かせるか",
                "設計",
                "medium",
                "注文 API の idempotency はどこまで保証し、何を保証しないのか。",
                "submit 境界で二重送信を抑止するが、projection の順序保証とは別問題。",
                "idempotency key は client retry や UI 二重送信への防波堤になるが、aggregateSeq や downstream replay の整合とは役割が違う。面接では duplicate suppression と ordering / exactly-once を混同しないことが重要。",
                List.of("/mobile/orders", "/mobile/architecture"),
                List.of(
                    "/Users/fujii/Desktop/dev/event-switchyard/gateway-rust/src/server/http/orders/classic.rs",
                    "/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/order/OrderService.java"
                ),
                List.of("idempotency", "duplicate suppression")
            ),
            new LearningCard(
                "event-id-vs-order-id",
                "eventId と orderId の違い",
                "設計",
                "medium",
                "なぜ eventId と orderId を分ける必要があるのか。",
                "orderId は aggregate identity、eventId は 1 イベント単位の重複排除キー。",
                "注文の identity とイベントの identity を分けないと、同一注文に紐づく複数イベントの重複排除や再送説明が破綻する。eventId は replay や DLQ 再投入時の追跡にも効く。",
                List.of("/mobile/architecture"),
                List.of(
                    "/Users/fujii/Desktop/dev/event-switchyard/contracts/bus_event_v2.schema.json",
                    "/Users/fujii/Desktop/dev/event-switchyard/oms-java/src/main/java/oms/audit/GatewayAuditIntakeService.java"
                ),
                List.of("eventId", "orderId", "dedup")
            ),
            new LearningCard(
                "venue-order-mapping",
                "clientOrderId と venueOrderId の対応",
                "注文",
                "medium",
                "内部注文 ID と venue order ID はどう扱うべきか。",
                "内部の業務識別と venue の session 識別を分ける。",
                "internal orderId は業務状態の anchor、venueOrderId は取引所 session の制御 ID。cancel や amend では venueOrderId の生存期間と再接続復元を区別して話す必要がある。",
                List.of("/mobile/orders", "/mobile/architecture"),
                List.of(
                    "/Users/fujii/Desktop/dev/event-switchyard/gateway-rust/src/exchange/client.rs",
                    "/Users/fujii/Desktop/dev/event-switchyard/gateway-rust/src/exchange/control.rs"
                ),
                List.of("clientOrderId", "venueOrderId")
            ),
            new LearningCard(
                "accepted-before-fill",
                "accepted より前に fill が見えたらどうするか",
                "運用",
                "hard",
                "fill-first を見たとき、なぜ即 apply してはいけないのか。",
                "accepted 前提が欠けるなら pending orphan に保留する。",
                "fill は会計的に重要でも、accepted 前提 없이 apply すると open order 数や reservation release と衝突する。pending orphan に置き、前提イベント到着後に replay するのが安全。",
                List.of("/mobile/architecture", "/mobile/ledger"),
                List.of(
                    "/Users/fujii/Desktop/dev/event-switchyard/oms-java/src/main/java/oms/audit/GatewayAuditIntakeService.java",
                    "/Users/fujii/Desktop/dev/event-switchyard/backoffice-java/src/main/java/backofficejava/audit/GatewayAuditIntakeService.java"
                ),
                List.of("fill-first", "pending orphan")
            ),
            new LearningCard(
                "offset-vs-checkpoint",
                "offset と aggregate progress の違い",
                "運用",
                "medium",
                "consumer offset が進んでいれば安全と言えない理由は何か。",
                "offset は読み取り位置、aggregate progress は適用済み sequence の位置。",
                "offset だけでは out-of-order や pending orphan が解けたかは分からない。aggregate progress とセットで見て初めて projection recovery を説明できる。",
                List.of("/mobile/architecture"),
                List.of(
                    "/Users/fujii/Desktop/dev/event-switchyard/oms-java/src/main/resources/db/migration/V3__oms_aggregate_progress.sql",
                    "/Users/fujii/Desktop/dev/event-switchyard/backoffice-java/src/main/resources/db/migration/V3__backoffice_aggregate_progress.sql"
                ),
                List.of("offset", "checkpoint", "aggregate progress")
            ),
            new LearningCard(
                "snapshot-vs-replay",
                "snapshot 復元と replay 復元の違い",
                "運用",
                "medium",
                "再起動時に snapshot だけでなく replay 導線も必要な理由は何か。",
                "snapshot は速いが、壊れた projection の説明責任は replay が持つ。",
                "projection の warm start は snapshot でよいが、不整合調査や drill では replay が必要。restore speed と explainability を分けて持つのが実務的。",
                List.of("/mobile/architecture"),
                List.of(
                    "/Users/fujii/Desktop/dev/event-switchyard/scripts/ops/drill_business_mainline_projection_recovery.sh",
                    "/Users/fujii/Desktop/dev/event-switchyard/docs/ops/business_mainline_operations_runbook.md"
                ),
                List.of("snapshot", "replay", "recovery")
            ),
            new LearningCard(
                "accounting-truth",
                "ledger が truth になる瞬間",
                "台帳",
                "medium",
                "どのタイミングで cash / position / pnl を確定させるべきか。",
                "注文 status ではなく fill 起点で ledger を起こす。",
                "accepted だけでは会計を確定できない。fill を受けて ledger entry を起こし、reservation release と合わせて cash / position / realized PnL を確定する。",
                List.of("/mobile/ledger"),
                List.of(
                    "/Users/fujii/Desktop/dev/event-switchyard/backoffice-java/src/main/java/backofficejava/ledger",
                    "/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/http/OrderApiHandler.java"
                ),
                List.of("ledger", "fill", "accounting truth")
            ),
            new LearningCard(
                "realized-vs-unrealized",
                "realized と unrealized の違い",
                "台帳",
                "easy",
                "realized PnL と unrealized PnL をどう分けて説明するか。",
                "realized は約定で確定、unrealized は保有ポジションの評価差。",
                "BackOffice の realized PnL は fills と平均取得価格から確定する。一方 unrealized は current price を使った評価差であり、mark-to-market 前提に依存する。",
                List.of("/mobile/ledger", "/mobile/risk"),
                List.of(
                    "/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/http/OrderApiHandler.java",
                    "/Users/fujii/Desktop/dev/event-switchyard/frontend/src/components/mobile/MobileOrderStudyView.tsx"
                ),
                List.of("realized", "unrealized", "mark-to-market")
            ),
            new LearningCard(
                "historical-var-reading",
                "historical VaR をどう読むか",
                "リスク",
                "medium",
                "historical VaR を見たとき、何を前提として確認すべきか。",
                "窓、信頼水準、保有期間、データ品質、モデルの単純化。",
                "教育用 historical VaR でも、何本の履歴を使ったか、何% tail か、価格系列がどの頻度かを先に確認する。数字の大きさだけではなく前提の薄さを話せることが重要。",
                List.of("/mobile/risk"),
                List.of(
                    "/Users/fujii/Desktop/dev/event-switchyard/frontend/src/components/mobile/MobileRiskView.tsx",
                    "/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/mobile/MobileLearningService.java"
                ),
                List.of("historical VaR", "confidence level")
            ),
            new LearningCard(
                "hedge-comparison",
                "simple hedge comparison の読み方",
                "リスク",
                "medium",
                "簡易 hedge comparison は何を見せ、何を見せないか。",
                "エクスポージャー圧縮の方向感は見せるが、執行コストや basis risk は見せない。",
                "この repo の hedge comparison は最大エクスポージャーを 50% 圧縮した教育用比較であり、最適ヘッジを解くものではない。reduction amount の意味と限界をセットで話す必要がある。",
                List.of("/mobile/risk"),
                List.of(
                    "/Users/fujii/Desktop/dev/event-switchyard/frontend/src/components/mobile/MobileRiskView.tsx",
                    "/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/mobile/MobileLearningService.java"
                ),
                List.of("hedge", "risk reduction")
            )
        );
    }

    private static List<RiskScenario> buildRiskScenarios() {
        return List.of(
            new RiskScenario(
                "market-down-3",
                "Market down 3%",
                "全ポジションに -3% shock を適用",
                null,
                -3.0,
                "portfolio",
                List.of("教育用の単純 market shock", "相関と流動性は未考慮", "reservation は margin ではない")
            ),
            new RiskScenario(
                "market-down-7",
                "Market down 7%",
                "全ポジションに -7% shock を適用",
                null,
                -7.0,
                "portfolio",
                List.of("教育用の単純 market shock", "保有期間と信頼水準は未定義", "realized / unrealized を分けて見る")
            ),
            new RiskScenario(
                "toyota-gap-down-12",
                "Toyota gap down 12%",
                "7203 に -12% shock を適用",
                "7203",
                -12.0,
                "single-name",
                List.of("single-name shock", "他銘柄は不変", "ギャップダウン時の集中リスク確認")
            ),
            new RiskScenario(
                "financials-down-9",
                "MUFG gap down 9%",
                "8306 に -9% shock を適用",
                "8306",
                -9.0,
                "single-name",
                List.of("single-name shock", "イベントドリブンの急変想定", "相関波及は省略")
            )
        );
    }

    public record MobileHomeResponse(
        String accountId,
        long generatedAt,
        ContinueLearning continueLearning,
        StudySuggestion todaySuggestion,
        MainlineStatus mainlineStatus,
        List<MobileOrderDigest> recentOrders,
        List<CardSummary> dueCards,
        List<CardSummary> bookmarks,
        List<QuickAction> quickActions,
        ProgressSummary progress
    ) {
    }

    public record ContinueLearning(
        String route,
        String orderId,
        String cardId,
        String title,
        String detail
    ) {
    }

    public record StudySuggestion(
        String label,
        String title,
        String detail,
        String route
    ) {
    }

    public record QuickAction(String label, String route, String tone) {
    }

    public record ProgressSummary(
        MobileProgressStore.LearningAnchor anchor,
        int dueCount,
        int bookmarkedCount,
        int completedCount
    ) {
    }

    public record MainlineStatus(
        boolean healthy,
        String summary,
        String omsState,
        String backOfficeState,
        int sequenceGapCount,
        int pendingOrphanCount,
        int deadLetterCount,
        List<String> focusNotes
    ) {
    }

    public record MobileOrderDigest(
        String orderId,
        String symbol,
        String symbolName,
        String status,
        long submittedAt,
        long filledQuantity,
        long remainingQuantity,
        String learningFocus
    ) {
    }

    public record LearningCard(
        String id,
        String title,
        String category,
        String difficulty,
        String question,
        String shortAnswer,
        String longAnswer,
        List<String> routes,
        List<String> codeReferences,
        List<String> keywords
    ) {
        public String route() {
            return routes.isEmpty() ? "/mobile/cards/" + id : routes.getFirst();
        }
    }

    public record CardProgress(
        String cardId,
        boolean bookmarked,
        boolean completed,
        int masteryLevel,
        int correctCount,
        int incorrectCount,
        long lastReviewedAt,
        long nextReviewAt
    ) {
    }

    public record CardSummary(
        String id,
        String title,
        String category,
        String difficulty,
        boolean bookmarked,
        boolean due,
        CardProgress progress,
        String route
    ) {
    }

    public record CardDetail(LearningCard card, CardProgress progress) {
    }

    public record ProgressResponse(
        String accountId,
        long updatedAt,
        MobileProgressStore.LearningAnchor anchor,
        int dueCount,
        int bookmarkedCount,
        int completedCount,
        List<CardSummary> cards
    ) {
    }

    public record ProgressUpdateRequest(
        String type,
        String route,
        String orderId,
        String cardId,
        Boolean bookmarked,
        Boolean correct
    ) {
    }

    public record RiskScenario(
        String id,
        String title,
        String description,
        String targetSymbol,
        double shockPercent,
        String scope,
        List<String> assumptions
    ) {
    }

    public record RiskEvaluationRequest(String scenarioId, String targetSymbol, Double customShockPercent) {
    }

    public record RiskEvaluationResponse(
        String accountId,
        String scenarioId,
        String title,
        String description,
        long evaluatedAt,
        PortfolioImpact portfolio,
        List<RiskPositionImpact> positions,
        HistoricalVar historicalVar,
        HedgeComparison hedgeComparison,
        List<String> assumptions
    ) {
    }

    public record PortfolioImpact(
        double currentMarketValue,
        double shockedMarketValue,
        double pnlDelta,
        long cashBalance,
        long realizedPnl
    ) {
    }

    public record RiskPositionImpact(
        String symbol,
        String symbolName,
        long netQty,
        double avgPrice,
        double currentPrice,
        double shockedPrice,
        double currentValue,
        double shockedValue,
        double pnlDelta,
        double shockPercent
    ) {
        public String shockLabel() {
            return String.format(Locale.US, "%+.1f%%", shockPercent);
        }
    }

    public record HistoricalVar(
        int confidenceLevel,
        int observationCount,
        double varLoss,
        double expectedShortfall,
        String holdingPeriod,
        String methodology
    ) {
    }

    public record HedgeComparison(
        String hedgeSymbol,
        double hedgeRatio,
        double unhedgedPnlDelta,
        double hedgedPnlDelta,
        double protectionAmount,
        String note
    ) {
    }
}
