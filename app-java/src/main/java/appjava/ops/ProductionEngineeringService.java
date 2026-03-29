package appjava.ops;

import appjava.clients.BackOfficeClient;
import appjava.clients.ExchangeSimulatorClient;
import appjava.clients.GatewayClient;
import appjava.clients.OmsClient;
import appjava.market.MarketDataService;
import appjava.order.OrderView;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public final class ProductionEngineeringService {
    private static final long MARKET_STALE_WARNING_MS = 3_000L;
    private static final long EVENT_STALE_WARNING_MS = 10_000L;
    private static final long GATEWAY_QUEUE_WARNING = 32L;
    private static final long GATEWAY_QUEUE_CRITICAL = 128L;
    private static final long GATEWAY_P99_WARNING_NS = 2_000_000L;
    private static final long GATEWAY_P99_CRITICAL_NS = 5_000_000L;

    private final String accountId;
    private final GatewayClient gatewayClient;
    private final ExchangeSimulatorClient exchangeSimulatorClient;
    private final MarketDataService marketDataService;
    private final OmsClient omsClient;
    private final BackOfficeClient backOfficeClient;

    public ProductionEngineeringService(
        String accountId,
        GatewayClient gatewayClient,
        ExchangeSimulatorClient exchangeSimulatorClient,
        MarketDataService marketDataService,
        OmsClient omsClient,
        BackOfficeClient backOfficeClient
    ) {
        this.accountId = accountId;
        this.gatewayClient = gatewayClient;
        this.exchangeSimulatorClient = exchangeSimulatorClient;
        this.marketDataService = marketDataService;
        this.omsClient = omsClient;
        this.backOfficeClient = backOfficeClient;
    }

    public ProductionEngineeringSnapshot buildSnapshot() {
        long generatedAt = System.currentTimeMillis();
        GatewayClient.GatewayHealth gatewayHealth = gatewayClient.fetchHealth().orElse(null);
        ExchangeSimulatorClient.ExchangeSimulatorStatus simulatorStatus = exchangeSimulatorClient.fetchStatus().orElse(null);
        OmsClient.OmsStats omsStats = omsClient.fetchStats();
        OmsClient.OmsBusStats omsBusStats = omsClient.fetchBusStats();
        OmsClient.OmsReconcile omsReconcile = omsClient.fetchReconcile(accountId);
        BackOfficeClient.BackOfficeStats backOfficeStats = backOfficeClient.fetchStats();
        BackOfficeClient.BackOfficeBusStats backOfficeBusStats = backOfficeClient.fetchBusStats();
        BackOfficeClient.BackOfficeReconcile backOfficeReconcile = backOfficeClient.fetchReconcile(accountId);
        MarketDataService.MarketFeedRuntimeSnapshot marketRuntime = marketDataService.getRuntimeSnapshot();

        GatewayRuntime gatewayRuntime = buildGatewayRuntime(gatewayHealth);
        ProjectionRuntime omsProjection = buildOmsProjection(generatedAt, omsStats, omsBusStats, omsReconcile);
        ProjectionRuntime backOfficeProjection = buildBackOfficeProjection(generatedAt, backOfficeStats, backOfficeBusStats, backOfficeReconcile);
        MarketDataRuntime marketDataRuntime = buildMarketDataRuntime(marketRuntime);
        SchemaRuntime schemaRuntime = buildSchemaRuntime(omsBusStats, backOfficeBusStats);
        CapacityRuntime capacityRuntime = buildCapacityRuntime(gatewayRuntime, omsProjection, backOfficeProjection, marketDataRuntime);
        VenueSessionsRuntime venueSessionsRuntime = buildVenueSessionsRuntime(gatewayRuntime, omsProjection, backOfficeProjection, marketDataRuntime, simulatorStatus);
        RolloutRuntime rolloutRuntime = buildRolloutRuntime(schemaRuntime, omsProjection, backOfficeProjection, marketDataRuntime, simulatorStatus);
        BooksRuntime booksRuntime = buildBooksRuntime(marketDataRuntime);
        List<IncidentSignal> incidents = buildIncidents(gatewayRuntime, omsProjection, backOfficeProjection, marketDataRuntime, schemaRuntime, capacityRuntime, simulatorStatus);
        GoNoGoRuntime goNoGoRuntime = buildGoNoGoRuntime(gatewayRuntime, omsProjection, backOfficeProjection, marketDataRuntime, schemaRuntime, venueSessionsRuntime, booksRuntime, simulatorStatus);

        return new ProductionEngineeringSnapshot(
            generatedAt,
            gatewayRuntime,
            omsProjection,
            backOfficeProjection,
            marketDataRuntime,
            schemaRuntime,
            capacityRuntime,
            venueSessionsRuntime,
            rolloutRuntime,
            booksRuntime,
            goNoGoRuntime,
            incidents,
            buildOperatorSequence(incidents)
        );
    }

    private GatewayRuntime buildGatewayRuntime(GatewayClient.GatewayHealth gatewayHealth) {
        if (gatewayHealth == null) {
            return new GatewayRuntime(
                "UNREACHABLE",
                null,
                -1L,
                -1L,
                -1L,
                List.of("gateway health を取得できない。受注経路の生存確認を最優先にする。")
            );
        }
        long queueLength = gatewayHealth.queueLen();
        long p99Ns = gatewayHealth.latencyP99Ns();
        String state = !"OK".equalsIgnoreCase(gatewayHealth.status())
            ? "DEGRADED"
            : queueLength >= GATEWAY_QUEUE_CRITICAL || p99Ns >= GATEWAY_P99_CRITICAL_NS
            ? "CRITICAL"
            : queueLength >= GATEWAY_QUEUE_WARNING || p99Ns >= GATEWAY_P99_WARNING_NS
            ? "DEGRADED"
            : "RUNNING";
        List<String> notes = new ArrayList<>();
        notes.add("queue=" + queueLength + " / p99=" + formatMicros(p99Ns) + " / p50=" + formatMicros(gatewayHealth.latencyP50Ns()));
        if (queueLength >= GATEWAY_QUEUE_WARNING) {
            notes.add("ingress queue が膨らんでいる。explanation logic を hot path に混ぜていないか確認する。");
        }
        if (p99Ns >= GATEWAY_P99_WARNING_NS) {
            notes.add("gateway p99 latency が伸びている。受理経路と projection 経路を切り分けて見る。");
        }
        return new GatewayRuntime(
            state,
            gatewayHealth.status(),
            queueLength,
            gatewayHealth.latencyP50Ns(),
            p99Ns,
            notes
        );
    }

    private ProjectionRuntime buildOmsProjection(
        long generatedAt,
        OmsClient.OmsStats stats,
        OmsClient.OmsBusStats busStats,
        OmsClient.OmsReconcile reconcile
    ) {
        List<String> notes = new ArrayList<>();
        if (reconcile != null && reconcile.issues() != null) {
            notes.addAll(reconcile.issues());
        }
        if (busStats != null && busStats.pending() > 0) {
            notes.add("OMS bus pending=" + busStats.pending());
        }
        if (busStats != null && busStats.errors() > 0) {
            notes.add("OMS bus errors=" + busStats.errors());
        }
        return new ProjectionRuntime(
            "OMS",
            safeState(stats == null ? null : stats.state()),
            stats == null ? 0 : stats.sequenceGaps(),
            stats == null ? 0 : stats.pendingOrphanCount(),
            stats == null ? 0 : stats.deadLetterCount(),
            stats == null ? 0 : stats.duplicates(),
            stats == null ? 0 : stats.replays(),
            ageMs(generatedAt, stats == null ? null : stats.lastEventAt()),
            safeState(busStats == null ? null : busStats.state()),
            busStats == null ? 0 : busStats.pending(),
            busStats == null ? 0 : busStats.errors(),
            stats == null ? 0 : stats.aggregateProgressCount(),
            notes.isEmpty() ? List.of("OMS projection issue なし") : notes.stream().limit(5).toList()
        );
    }

    private ProjectionRuntime buildBackOfficeProjection(
        long generatedAt,
        BackOfficeClient.BackOfficeStats stats,
        BackOfficeClient.BackOfficeBusStats busStats,
        BackOfficeClient.BackOfficeReconcile reconcile
    ) {
        List<String> notes = new ArrayList<>();
        if (reconcile != null && reconcile.issues() != null) {
            notes.addAll(reconcile.issues());
        }
        if (busStats != null && busStats.pending() > 0) {
            notes.add("BackOffice bus pending=" + busStats.pending());
        }
        if (busStats != null && busStats.errors() > 0) {
            notes.add("BackOffice bus errors=" + busStats.errors());
        }
        return new ProjectionRuntime(
            "BackOffice",
            safeState(stats == null ? null : stats.state()),
            stats == null ? 0 : stats.sequenceGaps(),
            stats == null ? 0 : stats.pendingOrphanCount(),
            stats == null ? 0 : stats.deadLetterCount(),
            stats == null ? 0 : stats.duplicates(),
            stats == null ? 0 : stats.replays(),
            ageMs(generatedAt, stats == null ? null : stats.lastEventAt()),
            safeState(busStats == null ? null : busStats.state()),
            busStats == null ? 0 : busStats.pending(),
            busStats == null ? 0 : busStats.errors(),
            stats == null ? 0 : stats.aggregateProgressCount(),
            notes.isEmpty() ? List.of("BackOffice projection issue なし") : notes.stream().limit(5).toList()
        );
    }

    private MarketDataRuntime buildMarketDataRuntime(MarketDataService.MarketFeedRuntimeSnapshot snapshot) {
        List<String> notes = new ArrayList<>();
        if (snapshot.staleSymbolCount() > 0) {
            notes.add("stale symbols=" + snapshot.staleSymbolCount() + " / budget=" + snapshot.freshnessBudgetMs() + "ms");
        }
        if (snapshot.venueAlertCount() > 0) {
            notes.add("auction / halt watch symbols=" + snapshot.venueAlertCount());
        }
        List<MarketSymbolRuntime> symbols = snapshot.symbols().stream()
            .sorted(Comparator.comparingLong(MarketDataService.MarketFeedSymbolRuntime::tickAgeMs).reversed())
            .map(symbol -> new MarketSymbolRuntime(
                symbol.symbol(),
                symbol.symbolName(),
                symbol.tickAgeMs(),
                symbol.venueState(),
                symbol.midPrice(),
                symbol.spreadBps()
            ))
            .toList();
        return new MarketDataRuntime(
            snapshot.state(),
            snapshot.freshnessBudgetMs(),
            snapshot.maxTickAgeMs(),
            snapshot.staleSymbolCount(),
            snapshot.venueAlertCount(),
            notes.isEmpty() ? List.of("market data freshness 良好") : notes,
            symbols
        );
    }

    private SchemaRuntime buildSchemaRuntime(OmsClient.OmsBusStats omsBusStats, BackOfficeClient.BackOfficeBusStats backOfficeBusStats) {
        long errors = (omsBusStats == null ? 0 : omsBusStats.errors()) + (backOfficeBusStats == null ? 0 : backOfficeBusStats.errors());
        long deadLetters = (omsBusStats == null ? 0 : omsBusStats.deadLetters()) + (backOfficeBusStats == null ? 0 : backOfficeBusStats.deadLetters());
        long pending = (omsBusStats == null ? 0 : omsBusStats.pending()) + (backOfficeBusStats == null ? 0 : backOfficeBusStats.pending());
        String state = errors > 0 || deadLetters > 0
            ? "MISMATCH_RISK"
            : pending > 0
            ? "WATCH"
            : "COMPATIBLE";
        List<String> notes = new ArrayList<>();
        notes.add("producer=bus_event_v2 / additive-only を維持");
        notes.add("consumer state: OMS=" + safeState(omsBusStats == null ? null : omsBusStats.state())
            + " / BackOffice=" + safeState(backOfficeBusStats == null ? null : backOfficeBusStats.state()));
        if (errors > 0) {
            notes.add("schema / decode errors=" + errors);
        }
        if (deadLetters > 0) {
            notes.add("dead letters=" + deadLetters);
        }
        return new SchemaRuntime(
            state,
            "bus_event_v2",
            pending,
            errors,
            deadLetters,
            notes
        );
    }

    private CapacityRuntime buildCapacityRuntime(
        GatewayRuntime gatewayRuntime,
        ProjectionRuntime omsProjection,
        ProjectionRuntime backOfficeProjection,
        MarketDataRuntime marketDataRuntime
    ) {
        long projectionBacklog = omsProjection.pendingOrphans()
            + omsProjection.deadLetters()
            + backOfficeProjection.pendingOrphans()
            + backOfficeProjection.deadLetters()
            + omsProjection.busPending()
            + backOfficeProjection.busPending();
        String state = "RUNNING";
        if ("CRITICAL".equals(gatewayRuntime.state()) || projectionBacklog > 25 || "STALE".equals(marketDataRuntime.state())) {
            state = "CRITICAL";
        } else if ("DEGRADED".equals(gatewayRuntime.state()) || projectionBacklog > 0 || "DEGRADED".equals(marketDataRuntime.state())) {
            state = "WATCH";
        }
        List<String> notes = new ArrayList<>();
        notes.add("gateway queue=" + gatewayRuntime.queueLength() + ", projection backlog=" + projectionBacklog);
        notes.add("market max tick age=" + marketDataRuntime.maxTickAgeMs() + "ms");
        if (projectionBacklog > 0) {
            notes.add("offset だけで正常と見なさず pending / DLQ / bus pending を同時に見る");
        }
        return new CapacityRuntime(
            state,
            gatewayRuntime.queueLength(),
            gatewayRuntime.latencyP99Ns(),
            projectionBacklog,
            notes
        );
    }

    private VenueSessionsRuntime buildVenueSessionsRuntime(
        GatewayRuntime gatewayRuntime,
        ProjectionRuntime omsProjection,
        ProjectionRuntime backOfficeProjection,
        MarketDataRuntime marketDataRuntime,
        ExchangeSimulatorClient.ExchangeSimulatorStatus simulatorStatus
    ) {
        String dropCopyState = simulatorStatus != null && simulatorStatus.dropCopyDivergence()
            ? "DIVERGED"
            : backOfficeProjection.deadLetters() > 0 || backOfficeProjection.pendingOrphans() > 0
            ? "LAGGING"
            : backOfficeProjection.lastEventAgeMs() > EVENT_STALE_WARNING_MS
            ? "WATCH"
            : "RUNNING";
        String throttleState = simulatorStatus != null && !"OPEN".equals(simulatorStatus.throttleState())
            ? simulatorStatus.throttleState()
            : gatewayRuntime.queueLength() >= GATEWAY_QUEUE_CRITICAL || gatewayRuntime.latencyP99Ns() >= GATEWAY_P99_CRITICAL_NS
            ? "HARD_LIMIT"
            : gatewayRuntime.queueLength() >= GATEWAY_QUEUE_WARNING || gatewayRuntime.latencyP99Ns() >= GATEWAY_P99_WARNING_NS
            ? "SOFT_LIMIT"
            : "OPEN";
        String entitlementState = simulatorStatus != null && !"FULL_ACCESS".equals(simulatorStatus.entitlementState())
            ? simulatorStatus.entitlementState()
            : ("CRITICAL".equals(dropCopyState) || "STALE".equals(marketDataRuntime.state()))
            ? "REDUCE_ONLY"
            : "FULL_ACCESS";
        List<VenueSession> sessions = List.of(
            new VenueSession(
                "Execution session",
                simulatorStatus == null ? gatewayRuntime.state() : simulatorStatus.sessionState(),
                simulatorStatus == null ? gatewayRuntime.healthStatus() : dropCopyState,
                simulatorStatus == null ? Math.max(0L, gatewayRuntime.queueLength()) : Math.max(0L, simulatorStatus.lastActivityAgeMs()),
                simulatorStatus == null
                    ? "queue=" + gatewayRuntime.queueLength() + " / p99=" + formatMicros(gatewayRuntime.latencyP99Ns())
                    : "mode=" + simulatorStatus.mode()
                    + " / working=" + simulatorStatus.workingOrders()
                    + " / lastActivity=" + simulatorStatus.lastActivityAgeMs() + "ms",
                simulatorStatus == null
                    ? List.of("gateway health", "venue heartbeat explanation", "cancel response lag")
                    : List.of(
                        "session=" + simulatorStatus.sessionState(),
                        "auction=" + simulatorStatus.auctionState(),
                        "submitted=" + simulatorStatus.submittedOrders(),
                        "rejected=" + simulatorStatus.rejectedOrders()
                    )
            ),
            new VenueSession(
                "Drop copy equivalent",
                dropCopyState,
                dropCopyState,
                simulatorStatus == null ? Math.max(0L, backOfficeProjection.lastEventAgeMs()) : Math.max(0L, simulatorStatus.lastActivityAgeMs()),
                "pending=" + backOfficeProjection.pendingOrphans()
                    + " / DLQ=" + backOfficeProjection.deadLetters()
                    + (simulatorStatus != null ? " / divergence=" + simulatorStatus.dropCopyDivergence() : ""),
                simulatorStatus == null
                    ? List.of("fills と ledger の同期", "dead letter", "pending orphan")
                    : mergeNotes(
                        List.of("fills と ledger の同期", "dead letter", "pending orphan"),
                        simulatorStatus.activeIncidents().isEmpty() ? List.of("simulator incident なし") : simulatorStatus.activeIncidents()
                    )
            )
        );
        return new VenueSessionsRuntime(sessions, throttleState, entitlementState);
    }

    private RolloutRuntime buildRolloutRuntime(
        SchemaRuntime schemaRuntime,
        ProjectionRuntime omsProjection,
        ProjectionRuntime backOfficeProjection,
        MarketDataRuntime marketDataRuntime,
        ExchangeSimulatorClient.ExchangeSimulatorStatus simulatorStatus
    ) {
        boolean replayReady = omsProjection.pendingOrphans() == 0
            && omsProjection.deadLetters() == 0
            && backOfficeProjection.pendingOrphans() == 0
            && backOfficeProjection.deadLetters() == 0;
        boolean consumerCompatible = !"MISMATCH_RISK".equals(schemaRuntime.state());
        boolean simulatorReady = simulatorStatus == null
            || ("RUNNING".equals(simulatorStatus.sessionState())
            && !simulatorStatus.dropCopyDivergence()
            && "FULL_ACCESS".equals(simulatorStatus.entitlementState()));
        String state = replayReady && consumerCompatible && simulatorReady ? "READY" : consumerCompatible ? "WATCH" : "BLOCKED";
        List<String> checks = new ArrayList<>();
        checks.add("contract=" + schemaRuntime.contractVersion());
        checks.add("consumer compatible=" + consumerCompatible);
        checks.add("replay ready=" + replayReady);
        checks.add("simulator ready=" + simulatorReady);
        if ("STALE".equals(marketDataRuntime.state())) {
            checks.add("feed stale 中は valuation guard を広げる");
        }
        if (simulatorStatus != null) {
            checks.add("sim mode=" + simulatorStatus.mode() + " / session=" + simulatorStatus.sessionState());
        }
        return new RolloutRuntime(
            state,
            schemaRuntime.contractVersion(),
            replayReady ? "READY" : "BLOCKED",
            consumerCompatible ? "COMPATIBLE" : "MISMATCH_RISK",
            checks
        );
    }

    private BooksRuntime buildBooksRuntime(MarketDataRuntime marketDataRuntime) {
        OrderView latestOrder = latestOrder();
        if (latestOrder == null) {
            return new BooksRuntime("NO_TRADE", "NO_TRADE", "READY", "NORMAL", List.of("直近注文が無い"));
        }
        BackOfficeClient.SettlementProjection settlementProjection = backOfficeClient.fetchSettlementProjection(latestOrder.id());
        BackOfficeClient.StatementProjection statementProjection = backOfficeClient.fetchStatementProjection(latestOrder.id());
        BackOfficeClient.MarginProjection marginProjection = backOfficeClient.fetchMarginProjection(accountId);
        String valuationGuard = "STALE".equals(marketDataRuntime.state())
            ? "WIDEN_GUARD"
            : marginProjection != null && !"WITHIN_LIMIT".equals(marginProjection.breachStatus())
            ? "RISK_GUARD"
            : "NORMAL";
        List<String> notes = new ArrayList<>();
        notes.add(settlementProjection == null ? "settlement projection なし" : "settlement=" + settlementProjection.settlementStatus());
        notes.add(statementProjection == null ? "statement projection なし" : "statement=" + statementProjection.statementStatus());
        if (marginProjection != null) {
            notes.add("margin=" + marginProjection.breachStatus() + " " + Math.round(marginProjection.utilizationPercent()) + "%");
        }
        return new BooksRuntime(
            settlementProjection == null ? "NO_TRADE" : settlementProjection.settlementStatus(),
            statementProjection == null ? "NO_TRADE" : statementProjection.statementStatus(),
            "READY",
            valuationGuard,
            notes
        );
    }

    private List<IncidentSignal> buildIncidents(
        GatewayRuntime gatewayRuntime,
        ProjectionRuntime omsProjection,
        ProjectionRuntime backOfficeProjection,
        MarketDataRuntime marketDataRuntime,
        SchemaRuntime schemaRuntime,
        CapacityRuntime capacityRuntime,
        ExchangeSimulatorClient.ExchangeSimulatorStatus simulatorStatus
    ) {
        List<IncidentSignal> incidents = new ArrayList<>();
        if ("UNREACHABLE".equals(gatewayRuntime.state()) || "CRITICAL".equals(gatewayRuntime.state())) {
            incidents.add(new IncidentSignal(
                "gateway-session",
                "P1",
                "受注 gateway の状態悪化",
                "queue/p99 が悪化している。新規受注遅延と venue 側説明不能の両方を疑う。",
                "gateway /health を基点に ingress 圧力と venue 接続の切り分けを最初に行う。"
            ));
        }
        if (omsProjection.sequenceGaps() > 0 || omsProjection.pendingOrphans() > 0) {
            incidents.add(new IncidentSignal(
                "oms-sequence-gap",
                "P1",
                "OMS sequence gap / pending orphan",
                "accepted 前提待ちで projection が止まる。status を勝手に丸めるのが最も危険。",
                "raw event 順序と aggregate progress を確認し、replay の前に pending と DLQ を分けて数える。"
            ));
        }
        if (backOfficeProjection.deadLetters() > 0 || backOfficeProjection.pendingOrphans() > 0) {
            incidents.add(new IncidentSignal(
                "backoffice-ledger-risk",
                "P1",
                "BackOffice projection 未収束",
                "cash / position / realized PnL の説明責任が閉じていない。",
                "fills と ledger の根拠イベントを照合し、DLQ と reconcile issue を同時に確認する。"
            ));
        }
        if ("STALE".equals(marketDataRuntime.state()) || marketDataRuntime.maxTickAgeMs() > MARKET_STALE_WARNING_MS) {
            incidents.add(new IncidentSignal(
                "market-data-stale",
                "P2",
                "market data stale",
                "execution quality と risk の両方が薄くなる。current price を信じてよいかが崩れる。",
                "stale 表示を前面に出し、benchmark と current price を混ぜない。"
            ));
        }
        if ("MISMATCH_RISK".equals(schemaRuntime.state())) {
            incidents.add(new IncidentSignal(
                "schema-compatibility",
                "P1",
                "schema / decode 互換リスク",
                "consumer が payload を解釈できず dead letter が増えている。",
                "additive-only 原則を確認し、互換 consumer 配布前に rollback しない。"
            ));
        }
        if ("CRITICAL".equals(capacityRuntime.state())) {
            incidents.add(new IncidentSignal(
                "capacity-pressure",
                "P2",
                "capacity / backlog pressure",
                "受理経路は生きていても projection 側が遅延し、final-out の説明が閉じにくい。",
                "queue と backlog を一緒に見て、offset 単独の正常判定をやめる。"
            ));
        }
        if (simulatorStatus != null && !"RUNNING".equals(simulatorStatus.sessionState())) {
            incidents.add(new IncidentSignal(
                "venue-session",
                "P1",
                "venue session 異常",
                "execution simulator の session が RUNNING ではない。新規受注や cancel 応答の意味が変わる。",
                "session state、auction state、entitlement を同時に確認する。"
            ));
        }
        if (simulatorStatus != null && simulatorStatus.dropCopyDivergence()) {
            incidents.add(new IncidentSignal(
                "drop-copy-divergence",
                "P1",
                "drop copy equivalent divergence",
                "執行経路と post-trade 経路の説明がずれている可能性がある。",
                "fills / ledger / divergence flag を突き合わせ、statement release を止める。"
            ));
        }
        if (simulatorStatus != null && "HALTED".equals(simulatorStatus.auctionState())) {
            incidents.add(new IncidentSignal(
                "auction-halt",
                "P2",
                "auction / halt mode",
                "板は見えても continuous trading ではない。arrival benchmark と fill quality の意味が変わる。",
                "auction/halt 表示を前面に出し、通常時 benchmark と混ぜない。"
            ));
        }
        if (incidents.isEmpty()) {
            incidents.add(new IncidentSignal(
                "steady-state",
                "OK",
                "現在は重大 incident なし",
                "session、feed、schema、projection が大きく崩れていない。",
                "drill を回して観測順を維持する。"
            ));
        }
        return incidents;
    }

    private GoNoGoRuntime buildGoNoGoRuntime(
        GatewayRuntime gatewayRuntime,
        ProjectionRuntime omsProjection,
        ProjectionRuntime backOfficeProjection,
        MarketDataRuntime marketDataRuntime,
        SchemaRuntime schemaRuntime,
        VenueSessionsRuntime venueSessionsRuntime,
        BooksRuntime booksRuntime,
        ExchangeSimulatorClient.ExchangeSimulatorStatus simulatorStatus
    ) {
        List<String> blockedReasons = new ArrayList<>();
        List<String> requiredDrills = new ArrayList<>();

        if ("UNREACHABLE".equals(gatewayRuntime.state()) || "CRITICAL".equals(gatewayRuntime.state())) {
            blockedReasons.add("gateway ingress が不安定");
            requiredDrills.add("gateway queue / latency drill");
        }
        if (omsProjection.sequenceGaps() > 0 || omsProjection.pendingOrphans() > 0 || omsProjection.deadLetters() > 0) {
            blockedReasons.add("OMS projection 未収束");
            requiredDrills.add("projection replay drill");
        }
        if (backOfficeProjection.pendingOrphans() > 0 || backOfficeProjection.deadLetters() > 0) {
            blockedReasons.add("BackOffice ledger 未収束");
            requiredDrills.add("ledger reconcile drill");
        }
        if ("STALE".equals(marketDataRuntime.state())) {
            blockedReasons.add("market data stale");
            requiredDrills.add("feed stale drill");
        }
        if ("MISMATCH_RISK".equals(schemaRuntime.state())) {
            blockedReasons.add("schema compatibility risk");
            requiredDrills.add("schema rollout drill");
        }
        if (!"FULL_ACCESS".equals(venueSessionsRuntime.entitlementState())) {
            blockedReasons.add("entitlement / reduce-only 制約");
        }
        if ("HARD_LIMIT".equals(venueSessionsRuntime.throttleState())) {
            blockedReasons.add("throttle hard limit");
            requiredDrills.add("capacity pressure drill");
        }
        if (simulatorStatus != null && (simulatorStatus.dropCopyDivergence() || !"RUNNING".equals(simulatorStatus.sessionState()))) {
            blockedReasons.add("venue / drop-copy control plane 異常");
            requiredDrills.add("incident matrix drill");
        }
        if (!"READY".equals(booksRuntime.settlementState()) && !"SETTLED".equals(booksRuntime.settlementState()) && !"NO_TRADE".equals(booksRuntime.settlementState())) {
            blockedReasons.add("settlement projection 未完了");
        }
        if (!"READY".equals(booksRuntime.statementState()) && !"RELEASED".equals(booksRuntime.statementState()) && !"NO_TRADE".equals(booksRuntime.statementState())) {
            blockedReasons.add("statement projection 未完了");
        }

        String state = blockedReasons.isEmpty()
            ? "GO"
            : blockedReasons.size() <= 2
            ? "CAUTION"
            : "NO_GO";
        List<String> rationale = blockedReasons.isEmpty()
            ? List.of("ingress / projection / feed / schema / books が大きく崩れていない")
            : blockedReasons;
        return new GoNoGoRuntime(state, rationale, requiredDrills.isEmpty() ? List.of("steady-state observation") : requiredDrills.stream().distinct().toList());
    }

    private static List<String> mergeNotes(List<String> left, List<String> right) {
        List<String> merged = new ArrayList<>(left);
        merged.addAll(right);
        return merged;
    }

    private List<String> buildOperatorSequence(List<IncidentSignal> incidents) {
        IncidentSignal primary = incidents.getFirst();
        if ("OK".equals(primary.severity())) {
            return List.of(
                "1. gateway / OMS / BackOffice の state を順に確認する",
                "2. market data freshness と venue state を確認する",
                "3. schema rollout や pending / DLQ をゼロで保てているかを見る"
            );
        }
        return List.of(
            "1. 現象を session / projection / schema / feed のどれに属するか決める",
            "2. raw event と aggregate progress を照合し、勝手に status を丸めない",
            "3. replay 前に pending orphan と DLQ を分離して数える",
            "4. final-out / ledger / market data のどの説明責任が開いたままかを明示する",
            "5. recovery 後に reconcile と live freshness を再確認する"
        );
    }

    private static long ageMs(long now, Long eventAt) {
        return eventAt == null ? -1L : Math.max(0L, now - eventAt);
    }

    private static String safeState(String value) {
        return value == null || value.isBlank() ? "UNKNOWN" : value;
    }

    private static String formatMicros(long nanos) {
        if (nanos < 0) {
            return "n/a";
        }
        return String.format(java.util.Locale.US, "%.2fus", nanos / 1_000.0);
    }

    private OrderView latestOrder() {
        return omsClient.fetchOrders().stream()
            .sorted(Comparator.comparingLong(OrderView::submittedAt).reversed())
            .findFirst()
            .orElse(null);
    }

    public record ProductionEngineeringSnapshot(
        long generatedAt,
        GatewayRuntime gateway,
        ProjectionRuntime omsProjection,
        ProjectionRuntime backOfficeProjection,
        MarketDataRuntime marketData,
        SchemaRuntime schema,
        CapacityRuntime capacity,
        VenueSessionsRuntime venueSessions,
        RolloutRuntime rollout,
        BooksRuntime books,
        GoNoGoRuntime goNoGo,
        List<IncidentSignal> incidents,
        List<String> operatorSequence
    ) {
    }

    public record GatewayRuntime(
        String state,
        String healthStatus,
        long queueLength,
        long latencyP50Ns,
        long latencyP99Ns,
        List<String> notes
    ) {
    }

    public record ProjectionRuntime(
        String name,
        String state,
        long sequenceGaps,
        int pendingOrphans,
        int deadLetters,
        long duplicates,
        long replays,
        long lastEventAgeMs,
        String busState,
        long busPending,
        long busErrors,
        int aggregateProgressCount,
        List<String> notes
    ) {
    }

    public record MarketDataRuntime(
        String state,
        long freshnessBudgetMs,
        long maxTickAgeMs,
        int staleSymbolCount,
        int venueAlertCount,
        List<String> notes,
        List<MarketSymbolRuntime> symbols
    ) {
    }

    public record MarketSymbolRuntime(
        String symbol,
        String symbolName,
        long tickAgeMs,
        String venueState,
        double midPrice,
        double spreadBps
    ) {
    }

    public record SchemaRuntime(
        String state,
        String contractVersion,
        long pendingCount,
        long errorCount,
        long deadLetterCount,
        List<String> notes
    ) {
    }

    public record CapacityRuntime(
        String state,
        long gatewayQueueLength,
        long gatewayLatencyP99Ns,
        long projectionBacklog,
        List<String> notes
    ) {
    }

    public record VenueSessionsRuntime(
        List<VenueSession> sessions,
        String throttleState,
        String entitlementState
    ) {
    }

    public record VenueSession(
        String name,
        String state,
        String dropCopyState,
        long heartbeatAgeMs,
        String currentValue,
        List<String> notes
    ) {
    }

    public record RolloutRuntime(
        String state,
        String contractVersion,
        String replayReadiness,
        String consumerCompatibility,
        List<String> checks
    ) {
    }

    public record BooksRuntime(
        String settlementState,
        String statementState,
        String booksAndRecordsState,
        String valuationGuard,
        List<String> notes
    ) {
    }

    public record GoNoGoRuntime(
        String state,
        List<String> blockedReasons,
        List<String> requiredDrills
    ) {
    }

    public record IncidentSignal(
        String code,
        String severity,
        String title,
        String summary,
        String firstAction
    ) {
    }
}
