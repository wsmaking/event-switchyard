import type {
  MobileAssetClassGuide,
  MobileImplementationAnchor,
  MobileInstitutionalFlow,
  MobileOperationsGuide,
  MobilePostTradeGuide,
  MobileRiskDeepDive,
} from '../types/mobile';
import { getOfflineMarketStructure, getOfflineOpsOverview, getOfflineOrderFinalOut, getOfflineOrders } from './mobileOfflineStore';

function anchor(title: string, path: string, focus: string, excerpt?: string | null): MobileImplementationAnchor {
  return {
    title,
    path,
    focus,
    excerpt: excerpt ?? null,
  };
}

function latestOrder() {
  return [...getOfflineOrders()].sort((left, right) => right.submittedAt - left.submittedAt)[0] ?? null;
}

function averageFillPrice(orderId: string | null) {
  if (!orderId) return 0;
  const finalOut = getOfflineOrderFinalOut(orderId);
  if (!finalOut.fills.length) return 0;
  const totalQuantity = finalOut.fills.reduce((sum, fill) => sum + fill.quantity, 0);
  if (totalQuantity <= 0) return 0;
  const weighted = finalOut.fills.reduce((sum, fill) => sum + fill.price * fill.quantity, 0);
  return Math.round((weighted / totalQuantity) * 100) / 100;
}

export function getOfflineMobileInstitutionalFlow(): MobileInstitutionalFlow {
  const order = latestOrder();
  const symbol = order?.symbol ?? '7203';
  const structure = getOfflineMarketStructure(symbol);
  const totalQuantity = Math.max((order?.quantity ?? 500) * 10, 3000);
  const arrivalMid = structure.midPrice;
  const firstSlice = Math.round(totalQuantity * 0.2);
  const secondSlice = Math.round(totalQuantity * 0.25);
  const thirdSlice = Math.round(totalQuantity * 0.3);
  const fourthSlice = totalQuantity - firstSlice - secondSlice - thirdSlice;

  return {
    generatedAt: Date.now(),
    anchorOrderId: order?.id ?? null,
    symbol,
    symbolName: structure.symbolName,
    clientIntent: `${structure.symbolName} を一撃でぶつけず、参加率を守りながら集めたい。`,
    executionStyles: [
      {
        name: 'DMA',
        useCase: '価格とタイミングを発注者が直接取りたい場面',
        businessRule: 'queue priority と cancel-replace の癖を理解していることが前提',
        systemImplication: 'clientOrderId / venueOrderId を短い周期で扱う必要がある',
        tradeoffs: ['柔軟だが判断負荷が高い', '市場状態が悪いと説明責任が個人に寄る'],
      },
      {
        name: 'Care Order',
        useCase: '顧客説明と流動性探索を両立したい場面',
        businessRule: '価格保護を優先し、板外流動性も探る',
        systemImplication: '親注文と子注文の理由を operator が辿れる必要がある',
        tradeoffs: ['丁寧だが手動判断が増える', '文脈メモが無いと再現不能になる'],
      },
      {
        name: 'Participation',
        useCase: '市場出来高に対する footprint を抑えたい場面',
        businessRule: '市場 volume に応じて child order を刻む',
        systemImplication: 'arrival benchmark、参加率、slice schedule を一体で持つ',
        tradeoffs: ['終日未了の可能性', '平時は最も説明しやすい'],
      },
    ],
    parentOrderPlan: {
      side: order?.side ?? 'BUY',
      totalQuantity,
      arrivalMidPrice: arrivalMid,
      targetParticipationPercent: 12,
      scheduleWindowMinutes: 45,
      chosenStyle: 'Participation',
      whyNotOtherChoices: [
        '成行一撃は spread と板厚に対して不利',
        'DMA 単独では裁量説明が重くなりすぎる',
        'Care order ほど人手前提にせず execution quality を残せる',
      ],
    },
    childOrders: [
      {
        id: 'slice-01',
        venueIntent: 'opening-liquidity',
        plannedQuantity: firstSlice,
        benchmarkPrice: arrivalMid,
        expectedFillPrice: structure.askPrice,
        expectedSlippageBps: Number((((structure.askPrice - arrivalMid) / Math.max(arrivalMid, 1)) * 10000).toFixed(1)),
        timeBucketLabel: '前場寄り',
        learningPoint: 'top-of-book 内で収まる数量に抑え、価格を飛ばさない',
      },
      {
        id: 'slice-02',
        venueIntent: 'queue-build',
        plannedQuantity: secondSlice,
        benchmarkPrice: arrivalMid,
        expectedFillPrice: Number((arrivalMid + structure.spread / 2).toFixed(2)),
        expectedSlippageBps: Number((((arrivalMid + structure.spread / 2 - arrivalMid) / Math.max(arrivalMid, 1)) * 10000).toFixed(1)),
        timeBucketLabel: '前場中盤',
        learningPoint: '並んで待つ局面と取りに行く局面を分ける',
      },
      {
        id: 'slice-03',
        venueIntent: 'liquidity-sweep',
        plannedQuantity: thirdSlice,
        benchmarkPrice: arrivalMid,
        expectedFillPrice: Number((structure.askPrice + structure.spread).toFixed(2)),
        expectedSlippageBps: Number((((structure.askPrice + structure.spread - arrivalMid) / Math.max(arrivalMid, 1)) * 10000).toFixed(1)),
        timeBucketLabel: '後場寄り',
        learningPoint: '出来高が戻る時間帯だけ agressive に取りにいく',
      },
      {
        id: 'slice-04',
        venueIntent: 'close-risk-control',
        plannedQuantity: fourthSlice,
        benchmarkPrice: arrivalMid,
        expectedFillPrice: Number((arrivalMid + structure.spread * 0.7).toFixed(2)),
        expectedSlippageBps: Number((((arrivalMid + structure.spread * 0.7 - arrivalMid) / Math.max(arrivalMid, 1)) * 10000).toFixed(1)),
        timeBucketLabel: '大引け前',
        learningPoint: 'close に向けて participation を少し上げて未約定残を閉じる',
      },
    ],
    allocationPlan: {
      blockBook: 'block-equities-book',
      averagePrice: Number((arrivalMid + structure.spread * 0.55).toFixed(2)),
      totalQuantity,
      allocations: [
        { targetBook: 'long-only-japan', quantity: Math.round(totalQuantity * 0.5), ratioPercent: 50, note: 'benchmark 追随を優先' },
        { targetBook: 'event-driven', quantity: Math.round(totalQuantity * 0.3), ratioPercent: 30, note: '材料イベント前で確保優先' },
        { targetBook: 'multi-strat', quantity: totalQuantity - Math.round(totalQuantity * 0.8), ratioPercent: 20, note: '残りは流動性と inventory を見て配賦' },
      ],
      settlementNote: '平均約定単価で配賦し、child fill 価格そのものではなく block average で説明する。',
      controlChecks: [
        'allocation 合計数量と parent fill 総数量の一致',
        'book 事情ではなく fill 実績に基づく配賦',
        'care と DMA 混在時も平均価格説明を一本化',
      ],
    },
    parentExecutionState: null,
    allocationState: null,
    accountHierarchy: {
      clientName: 'Japan Equity Alpha Fund',
      legalEntity: 'Switchyard Capital Japan',
      region: 'APAC',
      desk: 'Cash Equities',
      strategy: 'Participation / care hybrid',
      clearingBroker: 'Prime JP Clearing',
      custodian: 'Tokyo Custody Services',
      books: [
        {
          fund: 'Alpha Growth',
          book: 'long-only-japan',
          subAccount: 'alpha-long-01',
          trader: 'equities-pm',
          settlementLocation: 'JP',
          mandate: 'benchmark aware accumulation',
        },
        {
          fund: 'Event Opportunities',
          book: 'event-driven',
          subAccount: 'event-book-03',
          trader: 'event-pm',
          settlementLocation: 'JP',
          mandate: 'event inventory with liquidity guard',
        },
      ],
      permissions: [
        {
          role: 'sales-trader',
          scope: 'parent order',
          actions: ['child schedule adjust', 'pause', 'resume'],
          approvalRequired: false,
          note: 'schedule 変更までは desk 権限で実行可能',
        },
        {
          role: 'operations-control',
          scope: 'allocation / statement',
          actions: ['allocation release', 'statement hold', 'exception escalate'],
          approvalRequired: true,
          note: 'books and records 系は control sign-off 前提',
        },
      ],
      reportingLines: [
        'client mandate -> desk risk limit -> book allocation',
        'parent execution -> block average -> account statement',
      ],
      controlChecks: [
        'desk と settlement location が一致しているか',
        'allocation 対象 book に事前権限があるか',
        'care order の手動判断が audit trail に残るか',
      ],
    },
    operatorControlState: {
      workflowState: 'READY_FOR_ALLOCATION',
      escalationLevel: 'DESK',
      requiredApprovals: [
        {
          name: 'Average price release',
          role: 'operations-control',
          state: 'PENDING',
          reason: 'book average を statement へ反映する前提',
          nextAction: '配賦数量一致を確認して release',
        },
      ],
      acknowledgements: [
        {
          actor: 'sales-trader',
          action: 'participation を 12% に固定',
          atLabel: '09:18',
          note: '寄り付きの board depth を見て aggressive 化を抑制',
        },
      ],
      permittedActions: ['child schedule adjust', 'allocation draft', 'exception note add'],
      blockedActions: ['statement release', 'cash break resolve'],
      auditTrail: [
        'arrival benchmark 固定済み',
        'parent/child mapping 記録済み',
        'allocation release は control 待ち',
      ],
    },
    operatorChecks: [
      'arrival benchmark を親注文単位で固定しているか',
      'participation rate を market volume 変化に応じて見直した理由を言えるか',
      'allocation 合計数量と parent fill 総数量が一致しているか',
      'child slice ごとの aggressiveness を市場状態に結び付けられるか',
    ],
    implementationAnchors: [
      anchor('benchmark 保存', '/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/order/ExecutionBenchmarkStore.java', 'arrival benchmark を orderId ごとに保存する入口'),
      anchor('final-out 集約', '/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/http/OrderApiHandler.java', 'execution quality を含めて注文の説明を束ねる'),
      anchor('市場構造生成', '/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/market/MarketDataService.java', 'book / spread / venue state を生成する'),
    ],
  };
}

export function getOfflineMobilePostTradeGuide(): MobilePostTradeGuide {
  const order = latestOrder();
  const finalOut = getOfflineOrderFinalOut(order?.id ?? getOfflineOrders()[0]?.id ?? '');
  const grossNotional = finalOut.fills.reduce((sum, fill) => sum + fill.notional, 0);
  const commission = Math.max(80, Math.round(grossNotional * 0.00025));
  const exchangeFee = Math.max(20, Math.round(grossNotional * 0.00005));
  const taxes = order?.side === 'SELL' ? Math.max(10, Math.round(grossNotional * 0.00015)) : 0;
  const netCashMovement = finalOut.fills.reduce((sum, fill) => sum + (fill.side === 'BUY' ? -fill.notional : fill.notional), 0) - commission - exchangeFee - taxes;

  return {
    generatedAt: Date.now(),
    orderId: order?.id ?? null,
    symbol: order?.symbol ?? '7203',
    orderStatus: order?.status ?? 'FILLED',
    stages: [
      { name: 'Execution', owner: 'front office', purpose: '約定数量と平均単価を確定する', currentView: `${finalOut.fills.length} fills / 平均 ${averageFillPrice(order?.id ?? null)}`, whyItMatters: 'post-trade は fill から始まる' },
      { name: 'Allocation', owner: 'middle office', purpose: 'book / fund へ数量を配賦する', currentView: '3 book / average price allocation', whyItMatters: 'child fill と配賦は別の説明を持つ' },
      { name: 'Clearing', owner: 'clearing', purpose: 'trade record を清算向けに正規化する', currentView: 'trade date / quantity / price を整形', whyItMatters: 'front UI の表示と clearing record は同一ではない' },
      { name: 'Settlement', owner: 'back office', purpose: 'cash と securities の受け渡しを管理する', currentView: 'T+2 前提 / cash move 反映', whyItMatters: 'trade date と settlement date を混同しない' },
      { name: 'Books and Records', owner: 'finance & control', purpose: 'ledger / statement / confirm を揃える', currentView: `${finalOut.fills.length} fill / cash ${finalOut.balanceEffect.cashDelta.toLocaleString('ja-JP')}円`, whyItMatters: '説明責任を画面の外まで閉じる' },
    ],
    feeBreakdown: {
      grossNotional,
      commission,
      exchangeFee,
      taxes,
      netCashMovement,
      assumptions: [
        'commission は broker fee の教育用近似',
        'exchange fee は venue fee / rebate の簡易近似',
        'tax は side と市場差を省略した説明用近似',
      ],
    },
    statementPreview: {
      accountId: finalOut.accountOverview.accountId,
      symbol: finalOut.order.symbol,
      symbolName: finalOut.order.symbol,
      settledQuantity: finalOut.fills.reduce((sum, fill) => sum + fill.quantity, 0),
      averagePrice: averageFillPrice(order?.id ?? null),
      settlementDateLabel: 'T+2 想定',
      netCashMovementLabel: `${netCashMovement > 0 ? '+' : ''}${netCashMovement.toLocaleString('ja-JP')}円`,
      notes: [
        'statement は final-out より短く、法定表示の骨格を優先する',
        '平均約定単価と settlement date を一行で説明できる必要がある',
        'confirm は raw event ではなく、受け渡し可能な表現に寄せる',
      ],
    },
    settlementChecks: [
      { title: 'trade / settlement の区別', rule: '約定日と受渡日を混同しない', currentValue: 'T+2 想定' },
      { title: 'fees / taxes 控除', rule: 'gross と net の差を説明できる', currentValue: `${commission + exchangeFee + taxes}円` },
      { title: 'ledger truth', rule: 'fill 件数と ledger 根拠が揃う', currentValue: `${finalOut.rawEvents.length} raw events` },
    ],
    corporateActionHooks: [
      { name: 'Dividend', businessImpact: '権利落ちで avg price と評価損益の説明が変わる', systemImpact: 'position valuation と statement 文面の両方に影響' },
      { name: 'Split / Reverse Split', businessImpact: 'quantity と average price の変換が必要', systemImpact: 'order history は変えず position 表示を調整' },
      { name: 'Ticker Change / Merger', businessImpact: 'symbol identity が変わっても会計の連続性を保つ', systemImpact: 'asset master と reporting label を分けて持つ' },
    ],
    settlementProjection: null,
    statementProjection: null,
    settlementExceptionWorkflow: {
      workflowStatus: 'WATCH',
      exceptionType: 'Cash break watch',
      blockedStage: 'Books and Records',
      ageingLabel: 'T+1 14:30',
      rootCause: 'fee/tax 近似値と custodian cash 예상 값の差分を解消中',
      exceptionOwner: 'post-trade control',
      resolutionEtaLabel: '本日 EOD',
      cashBreakAmount: 12000,
      securitiesBreakQuantity: 0,
      cancelCorrectRequired: false,
      failAgingBucket: 'same-day investigation',
      nextAction: 'cash break 根拠を reconcile して statement release 判定へ進む',
      breakDetails: ['commission rounding 差分', 'custodian side の fee timing 差分'],
      controls: ['cash break と securities break を混同しない', 'control sign-off 前に statement を release しない'],
      operatorNotes: ['books and records 側で hold 中', 'EOD までに break 解消見込み'],
    },
    corporateActionWorkflow: {
      eventName: 'Dividend watch',
      workflowStatus: 'MONITORING',
      recordDateLabel: '2026-04-02',
      effectiveDateLabel: '2026-04-03',
      customerImpact: '権利落ち後は評価損益と statement 文面の説明が変わる',
      ledgerImpact: 'cash entitlement と position continuity を別 line で持つ',
      booksRecordImpact: 'statement note に dividend entitlement を追記',
      ledgerContinuityCheck: 'pre / post CA で book quantity continuity を確認',
      nextAction: 'record date 前に entitlement snapshot を固定する',
      controls: ['corporate action は order history を書き換えない', 'symbol rename と cash entitlement を同列に扱わない'],
    },
    accountHierarchy: getOfflineMobileInstitutionalFlow().accountHierarchy,
    operatorControlState: getOfflineMobileInstitutionalFlow().operatorControlState,
    implementationAnchors: [
      anchor('fills / ledger client', '/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/clients/BackOfficeClient.java', 'post-trade 説明のために fills と ledger を引く入口'),
      anchor('BackOffice ledger', '/Users/fujii/Desktop/dev/event-switchyard/backoffice-java/src/main/java/backofficejava/ledger', 'cash / position / realized PnL の正本'),
      anchor('final-out との接続', '/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/http/OrderApiHandler.java', 'front-to-back の照会を束ねる'),
    ],
  };
}

export function getOfflineMobileRiskDeepDive(): MobileRiskDeepDive {
  return {
    generatedAt: Date.now(),
    accountId: 'demo-001',
    marketValue: 4380000,
    cashBalance: 12840000,
    concentration: [
      { symbol: '7203', symbolName: 'トヨタ自動車', exposure: 1820000, weightPercent: 41.6, note: 'single-name 依存が強く stress と liquidity を同時に見る領域' },
      { symbol: '6758', symbolName: 'ソニーグループ', exposure: 1430000, weightPercent: 32.6, note: 'book の主因。hedge や削減の優先候補' },
      { symbol: '8306', symbolName: '三菱UFJフィナンシャル・グループ', exposure: 1130000, weightPercent: 25.8, note: '補助的な寄与。単体より相関で読む' },
    ],
    factorExposures: [
      { factor: 'Japan Auto', exposure: 1820000, limit: 2200000, utilizationPercent: 82.7, note: 'single-name 集中と sector beta が重なる' },
      { factor: 'Consumer Tech', exposure: 1430000, limit: 1800000, utilizationPercent: 79.4, note: 'market beta shock で寄与が大きい' },
      { factor: 'JPY rates sensitivity', exposure: 420000, limit: 1000000, utilizationPercent: 42.0, note: 'equity carry と funding 側の説明用近似' },
    ],
    liquidity: [
      { symbol: '7203', symbolName: 'トヨタ自動車', positionQuantity: 720, visibleTopOfBookQuantity: 360, participationPercent: 200, estimatedDaysToExit: 2.2, note: 'top-of-book に対して大きく複数セッション前提' },
      { symbol: '6758', symbolName: 'ソニーグループ', positionQuantity: 140, visibleTopOfBookQuantity: 180, participationPercent: 77.8, estimatedDaysToExit: 0.9, note: 'POV や care order が欲しい水準' },
      { symbol: '8306', symbolName: '三菱UFJフィナンシャル・グループ', positionQuantity: 950, visibleTopOfBookQuantity: 2200, participationPercent: 43.2, estimatedDaysToExit: 0.5, note: '通常の participation 範囲で説明しやすい' },
    ],
    liquidityBuckets: [
      { bucket: '0-1 day', grossExposure: 1130000, stressedExitDays: 0.6, action: '通常 participation で exit 可能' },
      { bucket: '1-3 days', grossExposure: 1430000, stressedExitDays: 1.8, action: 'care order か POV へ寄せる' },
      { bucket: '3+ days', grossExposure: 1820000, stressedExitDays: 3.4, action: 'desk review と inventory 方針確認' },
    ],
    scenarioLibrary: [
      { id: 'single-name-gap', title: '単一銘柄ギャップダウン', category: 'concentration', shock: '-12%', rationale: '材料イベントで one-name risk が顕在化', focus: 'single-name exposure と liquidity を同時に確認' },
      { id: 'market-beta-down', title: '市場全体の beta shock', category: 'portfolio', shock: '-5%', rationale: 'beta の高い book で同方向リスクが顕在化', focus: 'gross / net を一緒に説明' },
      { id: 'spread-widening', title: '流動性悪化と spread 拡大', category: 'liquidity', shock: '+40% spread', rationale: '価格そのものより execution cost が悪化', focus: 'arrival benchmark と fill quality を再評価' },
      { id: 'basis-break', title: 'ヘッジ不一致', category: 'hedge', shock: '相関崩れ', rationale: 'ヘッジ対象と実ポジションの基礎関係が崩れる', focus: 'hedge ratio だけで安心しない' },
    ],
    backtesting: {
      observationCount: 8,
      breachRatePercent: 25,
      averageTailLoss: 124000,
      note: '教育用 backtest。直近 tick を使い、stress breach を単純判定している。',
      samples: [
        { label: 'tick-8', pnl: -42000, breached: false },
        { label: 'tick-7', pnl: -98000, breached: true },
        { label: 'tick-6', pnl: 23000, breached: false },
        { label: 'tick-5', pnl: -151000, breached: true },
        { label: 'tick-4', pnl: 17000, breached: false },
        { label: 'tick-3', pnl: -66000, breached: false },
        { label: 'tick-2', pnl: 12000, breached: false },
        { label: 'tick-1', pnl: -31000, breached: false },
      ],
    },
    marginProjection: {
      methodology: 'portfolio stress add-on',
      marginLimit: 4200000,
      marginUsed: 3560000,
      utilizationPercent: 84.8,
      breachStatus: 'WATCH',
      breachedLimits: ['single-name concentration watch'],
      requiredActions: ['集中上位銘柄の execution plan を更新', 'liquidity add-on 前提を desk と共有'],
      modelNotes: ['教育用近似。real venue margin ではない', 'factor と liquidity add-on を同時に読む'],
      marginChangeDrivers: ['トヨタの concentration 増加', 'spread widening scenario の反映'],
      nextReviewWindowLabel: '次回 14:30 review',
    },
    scenarioEvaluationHistory: {
      lastEvaluatedAtLabel: '2026-03-29 10:42 JST',
      governanceState: 'REVIEWED',
      modelVersion: 'risk-lite-v3',
      approvals: ['desk risk reviewed', 'operations noted'],
      evaluations: [
        { title: 'market beta down', shock: '-5%', pnlDelta: -168000, note: 'beta 寄与が主' },
        { title: 'single-name gap', shock: '-12%', pnlDelta: -218000, note: 'concentration limit に接近' },
      ],
    },
    backtestHistory: {
      windowLabel: '直近 20 session',
      coverageLabel: 'Japan cash equities core book',
      breachRatePercent: 8.5,
      exceptions: ['2026-02-18 market halt day', '2026-03-03 spread widening event'],
      history: [
        { label: '2026-03-28', pnl: -42000, breached: false, note: '平常日' },
        { label: '2026-03-25', pnl: -98000, breached: true, note: 'gap open 後の breach' },
        { label: '2026-03-21', pnl: 23000, breached: false, note: 'hedge 効果あり' },
      ],
    },
    modelBoundaries: [
      { title: 'Concentration', whyItMatters: 'gross / net と weight を見る入口', whatIncluded: 'current price と保有数量', whatExcluded: '相関と beta decomposition' },
      { title: 'Liquidity', whyItMatters: 'exit difficulty を直感で掴む入口', whatIncluded: 'top-of-book と position size', whatExcluded: 'market impact model、queue depletion、venue fragmentation' },
      { title: 'Scenario Library', whyItMatters: 'shock を業務言語に変換する', whatIncluded: 'single-name / market / spread widening', whatExcluded: 'stochastic correlation、vol surface' },
      { title: 'Backtesting', whyItMatters: '前提が外れた頻度を掴む', whatIncluded: '簡易 breach count と tail loss', whatExcluded: 'regulatory backtesting や model governance' },
    ],
    governanceChecks: [
      { title: 'scenario library review', state: 'PASS', owner: 'desk risk', note: 'shock set が業務言語で維持されている' },
      { title: 'liquidity add-on review', state: 'WATCH', owner: 'operations-control', note: '3日超 bucket が増えている' },
    ],
    limitBreaches: [
      { limitName: 'single-name concentration', severity: 'MEDIUM', state: 'WATCH', nextAction: 'execution plan を participation 低めに再設定' },
    ],
    implementationAnchors: [
      anchor('risk 既存計算', '/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/mobile/MobileLearningService.java', 'shock / historical VaR / option evaluation の既存計算'),
      anchor('市場構造利用', '/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/market/MarketDataService.java', 'spread / depth を liquidity 説明の前提に使う'),
    ],
  };
}

export function getOfflineMobileAssetClassGuide(): MobileAssetClassGuide {
  return {
    generatedAt: Date.now(),
    assetClasses: [
      {
        assetClass: 'Equities',
        tradeInitiation: 'DMA / care / algo',
        lifecycle: 'accepted -> working -> partial/fill/cancel/expire',
        valuationDriver: 'last / bid-ask / spread / board depth',
        settlementModel: 'T+2 現物受渡',
        riskDriver: 'single-name concentration、beta、liquidity',
        operatorWatchpoints: ['queue priority', 'auction / halt', 'corporate action'],
        whatStaysCommon: ['order identity', 'audit trail', 'OMS / BackOffice 分離'],
        whatMustSpecialize: ['board depth', 'settlement', 'corporate action mapping'],
        booksAndRecordsImplications: ['final-out と statement を分ける', 'corporate action 前後で連続性を保つ'],
        failureModes: ['queue priority を見ずに一撃でぶつける', 'statement と final-out を同義に扱う'],
      },
      {
        assetClass: 'Options',
        tradeInitiation: 'listed option order / quote',
        lifecycle: 'quote -> order -> fill -> greek refresh',
        valuationDriver: 'vol surface、time to expiry、skew',
        settlementModel: 'premium と exercise / assignment',
        riskDriver: 'delta / gamma / vega / theta',
        operatorWatchpoints: ['series selection', 'expiry roll', 'exercise cutoff'],
        whatStaysCommon: ['order lifecycle', 'audit trail'],
        whatMustSpecialize: ['valuation engine', 'greeks refresh', 'exercise event'],
        booksAndRecordsImplications: ['premium と underlying を別で持つ', 'exercise 後に risk driver が変わる'],
        failureModes: ['現物の average price だけで説明する', 'exercise cutoff を無視する'],
      },
      {
        assetClass: 'FX',
        tradeInitiation: 'RFQ / streaming quote',
        lifecycle: 'RFQ / streaming quote -> fill -> cash ladder',
        valuationDriver: 'spot / forward points / carry',
        settlementModel: 'currency pair ごとの cash movement',
        riskDriver: 'spot exposure、carry、basis',
        operatorWatchpoints: ['session liquidity', 'cutoff time', 'settlement currency'],
        whatStaysCommon: ['execution intent', 'audit trail'],
        whatMustSpecialize: ['dual-currency ledger', 'holiday calendar', 'cutoff management'],
        booksAndRecordsImplications: ['dual-currency leg を分ける', 'holiday / cutoff を帳票に反映する'],
        failureModes: ['single-currency ledger に押し込む', 'cutoff と venue time を混同する'],
      },
      {
        assetClass: 'Rates',
        tradeInitiation: 'RFQ / voice assisted order',
        lifecycle: 'order / RFQ -> fill -> accrual / curve update',
        valuationDriver: 'yield curve、duration、convexity',
        settlementModel: 'coupon / accrual / settlement convention',
        riskDriver: 'DV01、curve shock、basis',
        operatorWatchpoints: ['day count', 'holiday convention', 'curve roll'],
        whatStaysCommon: ['position truth', 'operator action trace'],
        whatMustSpecialize: ['valuation conventions', 'curve build', 'cashflow schedule'],
        booksAndRecordsImplications: ['price と yield を同時に保持する', 'coupon accrual を別管理する'],
        failureModes: ['equity price のまま評価する', 'coupon accrual を settlement 後追いにする'],
      },
      {
        assetClass: 'Credit',
        tradeInitiation: 'axes / RFQ',
        lifecycle: 'axes / RFQ -> trade -> lifecycle events',
        valuationDriver: 'spread、hazard、recovery',
        settlementModel: 'coupon / default event / settlement convention',
        riskDriver: 'spread widening、jump-to-default',
        operatorWatchpoints: ['liquidity pockets', 'reference obligation', 'event handling'],
        whatStaysCommon: ['trade capture', 'audit', 'allocation'],
        whatMustSpecialize: ['event engine', 'default workflow', 'valuation source hierarchy'],
        booksAndRecordsImplications: ['default event を別 workflow に分ける', 'valuation source hierarchy を残す'],
        failureModes: ['credit event を symbol rename と同列に扱う', 'spread stale を equity stale と同じに扱う'],
      },
      {
        assetClass: 'Futures',
        tradeInitiation: 'listed futures order',
        lifecycle: 'accepted -> fill -> variation margin',
        valuationDriver: 'front / next contract curve、basis',
        settlementModel: 'daily settlement と margin call',
        riskDriver: 'basis、roll、liquidity concentration',
        operatorWatchpoints: ['roll schedule', 'session break', 'variation margin'],
        whatStaysCommon: ['order state', 'audit trail'],
        whatMustSpecialize: ['contract calendar', 'margin workflow', 'roll logic'],
        booksAndRecordsImplications: ['daily settlement と realized PnL を分ける', 'front と next を同じ symbol に畳まない'],
        failureModes: ['cash equities の受渡モデルを流用する', 'variation margin を statement 後追いにする'],
      },
    ],
    boundaryPrinciples: [
      '受注、状態遷移、監査の骨格は共通化できる',
      'valuation、risk driver、settlement convention は asset class ごとに専用化する',
      '共通化しすぎると金融の意味が消え、専用化しすぎると再利用性が消える',
      'books and records の列は似ていても、埋まる意味は商品ごとに違う',
    ],
    implementationAnchors: [
      anchor('市場構造画面', '/Users/fujii/Desktop/dev/event-switchyard/frontend/src/components/mobile/MobileMarketStructureView.tsx', 'equities の具体例として board / spread / slippage を示す'),
      anchor('risk / option 学習', '/Users/fujii/Desktop/dev/event-switchyard/frontend/src/components/mobile/MobileRiskView.tsx', '線形商品と option の差を同じ UI で示す'),
      anchor('設計カード', '/Users/fujii/Desktop/dev/event-switchyard/frontend/src/components/mobile/MobileCardsView.tsx', '共通化と専用化の判断を反復する'),
    ],
  };
}

export function getOfflineMobileOperationsGuide(): MobileOperationsGuide {
  const order = latestOrder();
  const ops = getOfflineOpsOverview(order?.id ?? null);
  return {
    generatedAt: Date.now(),
    liveState: {
      gatewayState: 'RUNNING',
      omsState: ops.omsStats?.state ?? 'ON_DEVICE',
      backOfficeState: ops.backOfficeStats?.state ?? 'ON_DEVICE',
      marketDataState: 'FRESH',
      schemaState: 'COMPATIBLE',
      capacityState: 'RUNNING',
      sequenceGapCount: (ops.omsStats?.sequenceGaps ?? 0) + (ops.backOfficeStats?.sequenceGaps ?? 0),
      pendingOrphanCount: ops.omsPendingOrphans.length + ops.backOfficePendingOrphans.length,
      deadLetterCount: ops.omsOrphans.length + ops.backOfficeOrphans.length,
      activeIncidentCount: 0,
      activeIncidents: ['active incident なし'],
      reconcileNotes: [...(ops.omsReconcile?.issues ?? []), ...(ops.backOfficeReconcile?.issues ?? [])].slice(0, 4).length
        ? [...(ops.omsReconcile?.issues ?? []), ...(ops.backOfficeReconcile?.issues ?? [])].slice(0, 4)
        : ['reconcile issue なし'],
    },
    sessionMonitors: [
      { name: 'Gateway / Venue session', state: 'RUNNING', whyItMatters: '受注は続いても venue 状態が悪いと執行説明が崩れる', currentValue: 'queue=0 / p99=0.30ms', checkpoints: ['heartbeat', 'cancel ack latency', 'venue state explanation'], operatorActions: ['gateway /health を起点に queue と latency を見る'] },
      { name: 'OMS projection', state: ops.omsStats?.state ?? 'ON_DEVICE', whyItMatters: 'accepted / cancel / fill の順序を守る壁', currentValue: `gap=${ops.omsStats?.sequenceGaps ?? 0} / pending=${ops.omsPendingOrphans.length} / DLQ=${ops.omsOrphans.length}`, checkpoints: ['aggregate progress', 'pending orphan', 'DLQ'], operatorActions: ['pending orphan と DLQ を分けて数える'] },
      { name: 'BackOffice projection', state: ops.backOfficeStats?.state ?? 'ON_DEVICE', whyItMatters: 'ledger truth と cash / position の正本', currentValue: `gap=${ops.backOfficeStats?.sequenceGaps ?? 0} / pending=${ops.backOfficePendingOrphans.length} / DLQ=${ops.backOfficeOrphans.length}`, checkpoints: ['fill count', 'ledger entries', 'reconcile issues'], operatorActions: ['ledger truth と fills の根拠 event を照合する'] },
      { name: 'Market data freshness', state: 'FRESH', whyItMatters: '価格 stale は execution と risk の両方を薄くする', currentValue: 'maxAge=1000ms / stale=0', checkpoints: ['tick freshness', 'venue state', 'price gap reason'], operatorActions: ['benchmark と current price を混ぜない'] },
    ],
    incidentDrills: [
      { name: 'sequence gap 増加', trigger: 'pending orphan が増え続ける', firstQuestions: ['accepted 前提が欠けているか', 'aggregate progress が止まっていないか'], actions: ['pending と DLQ を分けて数える', 'replay 前に raw event 順序を確認'], severity: 'P1', active: false, recoverySignal: 'pending orphan が解消し aggregate progress が進む' },
      { name: 'DLQ 増加', trigger: 'decode 失敗や schema mismatch', firstQuestions: ['payload が壊れているか', 'schema 想定が古いか'], actions: ['DLQ payload を保存', '互換性確認後に requeue'], severity: 'P1', active: false, recoverySignal: '同じ eventRef が再び DLQ に戻らない' },
      { name: 'market data stale', trigger: '価格は見えるが tick が止まる', firstQuestions: ['risk 数字が古くないか', 'arrival benchmark と current price が混ざっていないか'], actions: ['stale 表示を優先', 'execution / risk を保守モードで読む'], severity: 'P2', active: false, recoverySignal: 'fresh tick と venue state が復帰する' },
      { name: 'schema rollout', trigger: 'bus event 項目追加', firstQuestions: ['consumer 互換が保たれているか', 'default 値が定義されているか'], actions: ['backward compatible を先に配る', 'replay drill を事前実施'], severity: 'P2', active: false, recoverySignal: 'old/new payload の双方で projection が壊れない' },
    ],
    schemaControls: [
      { title: 'Event contract', rule: '新規項目は optional / additive を原則とする', failureMode: 'old consumer が decode 不能になり DLQ が急増する', currentState: 'COMPATIBLE', operatorChecks: ['producer=bus_event_v2', 'consumer が additive-only を前提にしているか'] },
      { title: 'Replay safety', rule: 'schema 変更時は replay script と checkpoint 互換を先に確認する', failureMode: '過去 audit が読めず recovery が失敗する', currentState: 'READY', operatorChecks: ['replay 前に pending / DLQ を分離する'] },
      { title: 'Operator wording', rule: 'UI 表示名を変えても raw event / source field は維持する', failureMode: 'runbook と現場画面の用語が噛み合わなくなる', currentState: 'REQUIRED', operatorChecks: ['accepted / filled / settlement を同義語にしない'] },
    ],
    capacityControls: [
      { title: 'Order intake latency', metric: 'hot path latency', threshold: '< 1ms class を維持', whyItMatters: '説明系 aggregation を gateway に混ぜない理由', currentValue: '0.30ms', status: 'RUNNING', operatorActions: ['queue と p99 を同時に見る'] },
      { title: 'Projection backlog', metric: 'pending orphan / DLQ / current offset', threshold: '増加傾向なら projection 側を疑う', whyItMatters: 'offset だけでは安全と言えない', currentValue: '0', status: 'RUNNING', operatorActions: ['offset 単独で正常判定しない'] },
      { title: 'Recovery time', metric: 'snapshot restore + replay duration', threshold: '数分以内を目標', whyItMatters: '事故後に再説明できるまでの時間を短くする', currentValue: 'drill ベース', status: 'WATCH', operatorActions: ['replay と reconcile を分けて確認する'] },
      { title: 'UI trust signal', metric: 'final-out completeness', threshold: 'fills / reservation / ledger が揃ってから成功判定', whyItMatters: '中途半端な画面で話を閉じない', currentValue: 'fills / reservation / ledger / reconcile OK', status: 'RUNNING', operatorActions: ['成功判定より説明責任の閉じ方を優先する'] },
    ],
    venueSessions: [
      { name: 'Execution session', state: 'RUNNING', dropCopyState: 'RUNNING', heartbeatAgeMs: 120, currentValue: 'queue=0 / p99=0.30ms', notes: ['gateway health', 'venue heartbeat explanation', 'cancel response lag'] },
      { name: 'Drop copy equivalent', state: 'RUNNING', dropCopyState: 'RUNNING', heartbeatAgeMs: 200, currentValue: 'pending=0 / DLQ=0', notes: ['fills と ledger の同期', 'dead letter', 'pending orphan'] },
    ],
    rolloutState: {
      state: 'READY',
      contractVersion: 'bus_event_v2',
      replayReadiness: 'READY',
      consumerCompatibility: 'COMPATIBLE',
      checks: ['contract=bus_event_v2', 'consumer compatible=true', 'replay ready=true'],
    },
    operatorSequence: [
      '1. 現象を session / projection / schema / feed のどれに属するか決める',
      '2. raw event と aggregate progress を照合し、勝手に status を丸めない',
      '3. replay 前に pending orphan と DLQ を分離して数える',
    ],
    guidedSteps: [
      '1. session / drop copy / entitlement を分けて見る',
      '2. replay readiness が READY か確認する',
      '3. settlement / statement / valuation guard まで閉じているか確認する',
    ],
    implementationAnchors: [
      anchor('ops overview 集約', '/Users/fujii/Desktop/dev/event-switchyard/app-java/src/main/java/appjava/http/OpsApiHandler.java', 'OMS / BackOffice / bus / orphan / DLQ をまとめる'),
      anchor('mainline runbook', '/Users/fujii/Desktop/dev/event-switchyard/docs/ops/business_mainline_operations_runbook.md', '事故時の確認順を文章で固定している'),
      anchor('projection recovery drill', '/Users/fujii/Desktop/dev/event-switchyard/scripts/ops/drill_business_mainline_projection_recovery.sh', 'replay と reconcile を分けて確認する drill'),
    ],
  };
}
