//! 動的 inflight 制御（single-writer）
//!
//! このモジュールの目的は、過負荷時に「未完了作業の積み上がり」を抑えること。
//! 入口で受け付け続けると、WAL遅延・キュー滞留・tail latency悪化が連鎖するため、
//! 受付可否を中央で一元判定し、必要時は早期拒否へ切り替える。
//!
//! 制御の要点:
//! - `reserve`: 今受けてもよいかを判定し、許可時のみ inflight を増やす
//! - `release`/`commit`: 完了を反映して inflight を減らす
//! - `tick`: commit実績(EWMA)から許容量(limit)を動的更新する
//!
//! これにより、スループットを維持しつつ p99 崩壊を避ける「入口の弁」として機能する。

use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, watch};

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub enum ReserveDecision {
    /// 受付許可。現在の負荷状況を呼び出し側に返す。
    Allow { inflight: u64, limit: u64 },
    /// 未完了件数が許容量を超えているため受付拒否。
    RejectInflight { inflight: u64, limit: u64 },
    /// durable処理の低下兆候を検知したため予防的に受付拒否。
    RejectRateDecline { inflight: u64, limit: u64 },
}

#[derive(Clone)]
pub struct InflightControllerHandle {
    enabled: bool,
    cmd_tx: mpsc::UnboundedSender<InflightCommand>,
    inflight_rx: watch::Receiver<u64>,
    limit_rx: watch::Receiver<u64>,
    rate_ewma_milli_rx: watch::Receiver<u64>,
    soft_reject_rate_milli_rx: watch::Receiver<u64>,
    #[allow(dead_code)]
    rate_declining_rx: watch::Receiver<bool>,
}

impl InflightControllerHandle {
    /// 動的 inflight 制御が有効かを返す。
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// 現在の inflight 件数（観測値）を返す。
    pub fn inflight(&self) -> u64 {
        *self.inflight_rx.borrow()
    }

    /// 現在の inflight 上限（観測値）を返す。
    pub fn current_limit(&self) -> u64 {
        *self.limit_rx.borrow()
    }

    /// commit throughput の EWMA（milli単位）を返す。
    pub fn rate_ewma_milli(&self) -> u64 {
        *self.rate_ewma_milli_rx.borrow()
    }

    /// soft reject rate の EWMA（milli単位）を返す。
    pub fn soft_reject_rate_ewma_milli(&self) -> u64 {
        *self.soft_reject_rate_milli_rx.borrow()
    }

    #[allow(dead_code)]
    /// throughput 低下中フラグ（観測値）を返す。
    pub fn rate_declining(&self) -> bool {
        *self.rate_declining_rx.borrow()
    }

    /// 新規受付可否を問い合わせる。
    /// 呼び出し側が直接カウンタを触らないことで、判定と更新の整合を保つ。
    pub async fn reserve(&self) -> ReserveDecision {
        // 呼び出し側は shared state を直接触らず、single-writer へ問い合わせる。
        let (tx, rx) = oneshot::channel();
        let _ = self.cmd_tx.send(InflightCommand::Reserve { resp: tx });
        // 応答不能時は安全側で拒否。
        rx.await.unwrap_or(ReserveDecision::RejectInflight {
            inflight: self.inflight(),
            limit: self.current_limit(),
        })
    }

    /// 完了通知を送る。未完了件数を減らし、詰まりを解消する方向に働く。
    pub fn release(&self, n: u64) {
        let _ = self.cmd_tx.send(InflightCommand::Release { n });
    }

    /// durable完了通知を送る。
    /// inflight減算に加え、throughput観測値(commit rate)を進める。
    pub fn on_commit(&self, n: u64) {
        let _ = self.cmd_tx.send(InflightCommand::Commit { n });
    }
}

#[derive(Debug)]
enum InflightCommand {
    Reserve {
        resp: oneshot::Sender<ReserveDecision>,
    },
    Release {
        n: u64,
    },
    Commit {
        n: u64,
    },
}

pub struct InflightControllerConfig {
    // limit ~= alpha * throughput * target_wal_age_sec
    // つまり「処理実力」と「許容遅延」から未完了許容量を決める。
    pub enabled: bool,
    pub target_wal_age_sec: f64,
    pub safety_alpha: f64,
    pub beta: f64,
    pub inflight_min: u64,
    pub inflight_cap: u64,
    pub tick_ms: u64,
    pub max_step_ratio: f64,
    pub hysteresis_off_ratio: f64,
    pub initial_limit: u64,
    pub decline_ratio: f64,
    pub decline_streak: u64,
    pub reject_rate_beta: f64,
}

impl InflightControllerConfig {
    /// 環境変数から制御パラメータを読み込む。
    /// 運用時の負荷特性に合わせて、制御の強さをチューニングできる。
    pub fn from_env() -> Self {
        // 1) on/off とサンプリング周期
        // - enabled: 動的制御そのものを有効化するスイッチ
        // - tick_ms: limit再計算の周期。短いほど追従性↑/ノイズ耐性↓
        let enabled = parse_bool_env("BACKPRESSURE_INFLIGHT_DYNAMIC").unwrap_or(false);
        let tick_ms = parse_u64_env("BACKPRESSURE_INFLIGHT_TICK_MS").unwrap_or(250);

        // 2) desired limit 計算 (tick内)
        // desired ~= alpha * rate_ewma * target_wal_age_sec
        // - target_wal_age_sec: 許容したいWAL年齢(遅延目標)
        // - safety_alpha: 安全係数。小さいほど保守的(早めに絞る)
        // - beta: rate EWMA の平滑化係数。大きいほど追従性↑
        let target_wal_age_sec =
            parse_f64_env("BACKPRESSURE_INFLIGHT_TARGET_WAL_AGE_SEC").unwrap_or(1.0);
        let safety_alpha = parse_f64_env("BACKPRESSURE_INFLIGHT_ALPHA").unwrap_or(0.8);
        let beta = parse_f64_env("BACKPRESSURE_INFLIGHT_BETA").unwrap_or(0.2);

        // 3) limit の物理的な可動範囲と変化速度 (tick内)
        // - inflight_min/cap: limitの下限/上限
        // - max_step_ratio: 1tickあたりの増減幅制限(振動抑制)
        let inflight_min = parse_u64_env("BACKPRESSURE_INFLIGHT_MIN").unwrap_or(256);
        let inflight_cap = parse_u64_env("BACKPRESSURE_INFLIGHT_CAP").unwrap_or(10_000);
        let max_step_ratio = parse_f64_env("BACKPRESSURE_INFLIGHT_SLEW_RATIO").unwrap_or(0.2);

        // 4) reserve時の拒否制御
        // - initial_limit: 起動直後のlimit初期値
        // - hysteresis_off_ratio: rejecting解除しきい値(再受理を遅らせる)
        let initial_limit = parse_u64_env("BACKPRESSURE_INFLIGHT_INITIAL").unwrap_or(2_000);
        let hysteresis_off_ratio =
            parse_f64_env("BACKPRESSURE_INFLIGHT_HYSTERESIS_OFF_RATIO").unwrap_or(0.9);

        // 5) 「処理能力低下」検知 (tick -> reserve)
        // - decline_ratio: rate_ewma が前回比でどこまで落ちたら低下とみなすか
        // - decline_streak: 低下連続回数しきい値
        let decline_ratio = parse_f64_env("BACKPRESSURE_INFLIGHT_DECLINE_RATIO").unwrap_or(0.8);
        let decline_streak = parse_u64_env("BACKPRESSURE_INFLIGHT_DECLINE_STREAK").unwrap_or(3);

        // 6) soft reject rate の観測平滑化 (メトリクス用途)
        let reject_rate_beta = parse_f64_env("BACKPRESSURE_INFLIGHT_REJECT_BETA").unwrap_or(0.2);

        Self {
            enabled,
            target_wal_age_sec,
            safety_alpha,
            beta,
            inflight_min,
            inflight_cap,
            tick_ms,
            max_step_ratio,
            hysteresis_off_ratio,
            initial_limit,
            decline_ratio,
            decline_streak,
            reject_rate_beta,
        }
    }
}

pub struct InflightController {
    config: InflightControllerConfig,
    cmd_rx: mpsc::UnboundedReceiver<InflightCommand>,
    inflight: u64,
    limit_tx: watch::Sender<u64>,
    inflight_tx: watch::Sender<u64>,
    rate_ewma_milli_tx: watch::Sender<u64>,
    soft_reject_rate_milli_tx: watch::Sender<u64>,
    rate_declining_tx: watch::Sender<bool>,
    last_sample: Instant,
    last_committed_total: u64,
    committed_total: u64,
    soft_reject_total: u64,
    rate_ewma: f64,
    reject_rate_ewma: f64,
    last_soft_reject_total: u64,
    last_rate_ewma: f64,
    decline_streak: u64,
    rejecting: bool,
}

impl InflightController {
    /// single-writer controller を起動して Handle を返す。
    /// 制御の意思決定を1箇所に寄せ、競合や順序ずれを避ける。
    pub fn spawn_from_env() -> InflightControllerHandle {
        let config = InflightControllerConfig::from_env();
        let initial_limit = config.initial_limit.max(1);
        // watch は read-mostly な観測値共有に使う。
        let (limit_tx, limit_rx) = watch::channel(initial_limit);
        let (inflight_tx, inflight_rx) = watch::channel(0u64);
        let (rate_ewma_milli_tx, rate_ewma_milli_rx) = watch::channel(0u64);
        let (soft_reject_rate_milli_tx, soft_reject_rate_milli_rx) = watch::channel(0u64);
        let (rate_declining_tx, rate_declining_rx) = watch::channel(false);
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        let handle = InflightControllerHandle {
            enabled: config.enabled,
            cmd_tx,
            inflight_rx,
            limit_rx,
            rate_ewma_milli_rx,
            soft_reject_rate_milli_rx,
            rate_declining_rx,
        };

        let controller = Self::new(
            config,
            cmd_rx,
            limit_tx,
            inflight_tx,
            rate_ewma_milli_tx,
            soft_reject_rate_milli_tx,
            rate_declining_tx,
        );
        tokio::spawn(async move {
            controller.run().await;
        });

        handle
    }

    #[allow(clippy::too_many_arguments)]
    /// controller 内部状態を初期化する。
    /// 初期rateを置くことで、起動直後から極端なlimit振れを抑える。
    fn new(
        config: InflightControllerConfig,
        cmd_rx: mpsc::UnboundedReceiver<InflightCommand>,
        limit_tx: watch::Sender<u64>,
        inflight_tx: watch::Sender<u64>,
        rate_ewma_milli_tx: watch::Sender<u64>,
        soft_reject_rate_milli_tx: watch::Sender<u64>,
        rate_declining_tx: watch::Sender<bool>,
    ) -> Self {
        // 初期rateは limit/target で近似。
        let target = config.target_wal_age_sec.max(0.001);
        let rate_ewma = (config.initial_limit as f64) / target;
        let _ = rate_ewma_milli_tx.send((rate_ewma * 1000.0) as u64);
        Self {
            config,
            cmd_rx,
            inflight: 0,
            limit_tx,
            inflight_tx,
            rate_ewma_milli_tx,
            soft_reject_rate_milli_tx,
            rate_declining_tx,
            last_sample: Instant::now(),
            last_committed_total: 0,
            committed_total: 0,
            soft_reject_total: 0,
            rate_ewma,
            reject_rate_ewma: 0.0,
            last_soft_reject_total: 0,
            last_rate_ewma: rate_ewma,
            decline_streak: 0,
            rejecting: false,
        }
    }

    async fn run(mut self) {
        // コマンド処理と周期制御を single-writer で直列実行する。
        let mut interval = tokio::time::interval(Duration::from_millis(self.config.tick_ms));
        loop {
            tokio::select! {
                maybe_cmd = self.cmd_rx.recv() => {
                    match maybe_cmd {
                        Some(cmd) => self.handle_cmd(cmd),
                        None => break,
                    }
                }
                _ = interval.tick() => {
                    self.tick();
                }
            }
        }
    }

    /// コマンドを直列処理し、状態更新と返信を行う。
    /// Reserve/Release/Commitを同じ文脈で処理することで整合を保つ。
    fn handle_cmd(&mut self, cmd: InflightCommand) {
        match cmd {
            InflightCommand::Reserve { resp } => {
                let decision = self.reserve();
                let _ = resp.send(decision);
            }
            InflightCommand::Release { n } => {
                self.inflight = self.inflight.saturating_sub(n);
                let _ = self.inflight_tx.send(self.inflight);
            }
            InflightCommand::Commit { n } => {
                // commit は inflight 減算に加え、throughput算出の入力にもなる。
                self.inflight = self.inflight.saturating_sub(n);
                self.committed_total = self.committed_total.saturating_add(n);
                let _ = self.inflight_tx.send(self.inflight);
            }
        }
    }

    /// 現在状態に基づいて新規受付可否を判定する。
    /// ここが「受ける/弾く」を決める入口バルブ本体。
    fn reserve(&mut self) -> ReserveDecision {
        let limit = *self.limit_tx.borrow();
        let inflight = self.inflight;
        let off_threshold = (limit as f64 * self.config.hysteresis_off_ratio) as u64;

        if !self.config.enabled {
            // 動的制御無効時は単純カウンタとして動く。
            self.inflight = self.inflight.saturating_add(1);
            let _ = self.inflight_tx.send(self.inflight);
            return ReserveDecision::Allow {
                inflight: self.inflight,
                limit,
            };
        }

        if *self.rate_declining_tx.borrow() {
            // durable rate 低下中は新規受付を抑制する。
            self.soft_reject_total = self.soft_reject_total.saturating_add(1);
            return ReserveDecision::RejectRateDecline { inflight, limit };
        }

        if self.rejecting {
            // ヒステリシス: 一度rejectingに入ると余裕が戻るまで拒否継続。
            if inflight >= off_threshold {
                self.soft_reject_total = self.soft_reject_total.saturating_add(1);
                return ReserveDecision::RejectInflight { inflight, limit };
            }
            self.rejecting = false;
        }

        if inflight >= limit {
            self.rejecting = true;
            self.soft_reject_total = self.soft_reject_total.saturating_add(1);
            return ReserveDecision::RejectInflight { inflight, limit };
        }

        self.inflight = self.inflight.saturating_add(1);
        let _ = self.inflight_tx.send(self.inflight);
        ReserveDecision::Allow {
            inflight: self.inflight,
            limit,
        }
    }

    /// commit/soft-reject の観測値から EWMA と limit を周期更新する。
    /// 負荷に追従して許容量を上下させ、崩壊前に受け付けを絞る。
    fn tick(&mut self) {
        let now = Instant::now();
        let dt = now.duration_since(self.last_sample);
        if dt < Duration::from_millis(50) {
            return;
        }

        let dt_sec = dt.as_secs_f64().max(0.000_001);
        let committed_delta = self
            .committed_total
            .saturating_sub(self.last_committed_total);
        let instant_rate = (committed_delta as f64) / dt_sec;

        // throughput EWMA を更新。
        self.rate_ewma =
            (1.0 - self.config.beta) * self.rate_ewma + self.config.beta * instant_rate;
        let _ = self
            .rate_ewma_milli_tx
            .send((self.rate_ewma * 1000.0) as u64);

        // 急落が連続した場合のみ rate_declining=true にする。
        if self.rate_ewma < self.last_rate_ewma * self.config.decline_ratio {
            self.decline_streak = self.decline_streak.saturating_add(1);
        } else {
            self.decline_streak = 0;
        }
        let _ = self
            .rate_declining_tx
            .send(self.decline_streak >= self.config.decline_streak);
        self.last_rate_ewma = self.rate_ewma;

        // soft reject rate EWMA を更新。
        let soft_reject_delta = self
            .soft_reject_total
            .saturating_sub(self.last_soft_reject_total);
        let instant_reject_rate = (soft_reject_delta as f64) / dt_sec;
        self.reject_rate_ewma = (1.0 - self.config.reject_rate_beta) * self.reject_rate_ewma
            + self.config.reject_rate_beta * instant_reject_rate;
        let _ = self
            .soft_reject_rate_milli_tx
            .send((self.reject_rate_ewma * 1000.0) as u64);
        self.last_soft_reject_total = self.soft_reject_total;

        if self.config.enabled {
            // Little's Law 系で desired limit を計算。
            let target = self.config.target_wal_age_sec.max(0.001);
            let raw = self.config.safety_alpha * self.rate_ewma * target;
            let mut desired = raw.round() as u64;
            desired = desired.clamp(self.config.inflight_min, self.config.inflight_cap);

            // 1tickあたりの変化量を制限して振動を抑える。
            let current = *self.limit_tx.borrow();
            let max_step = ((current as f64) * self.config.max_step_ratio).max(1.0) as u64;
            let next = if desired > current + max_step {
                current + max_step
            } else if desired + max_step < current {
                current.saturating_sub(max_step)
            } else {
                desired
            };
            if next != current {
                let _ = self.limit_tx.send(next);
            }
        }

        self.last_committed_total = self.committed_total;
        self.last_sample = now;
    }
}

/// 環境変数を u64 として解釈する。
fn parse_u64_env(key: &str) -> Option<u64> {
    std::env::var(key).ok().and_then(|v| v.parse::<u64>().ok())
}

/// 環境変数を f64 として解釈する。
fn parse_f64_env(key: &str) -> Option<f64> {
    std::env::var(key).ok().and_then(|v| v.parse::<f64>().ok())
}

/// 環境変数を bool として解釈する（1/true/yes/on, 0/false/no/off）。
fn parse_bool_env(key: &str) -> Option<bool> {
    std::env::var(key)
        .ok()
        .and_then(|v| match v.to_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
}
