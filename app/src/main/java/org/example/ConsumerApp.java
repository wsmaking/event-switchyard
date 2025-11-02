package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.HdrHistogram.Histogram;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 計測付きKafkaコンシューマ: E2E遅延をHDR Histogramに記録（p50/p95/p99/p99.9）
 * 
 * スレッド構成:
 * - メインスレッド: Kafkaメッセージ処理
 * - schedulerスレッド: 定期監視（TPS + ウィンドウp99）
 * - shutdown hookスレッド: 安全終了処理
 * 
 * Lag測定の責務分離:
 * - 件数ベースLag: 統計計算専用（lagSamples, lagSum, lagMax更新）
 * - 時間ベースLag: 可視化用JSON記録専用
 * - 終了時Lag: 最終状態確認専用
 */
public class ConsumerApp {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerApp.class);

    // スレッド間共有: TPS計算用カウンタ（AtomicLongで競合回避）
    private static final AtomicLong messageCount = new AtomicLong(0);

    // 別スレッド作成: 定期タスク実行用
    private static final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // 遅延分布（全期間）: 最小1µs、最大5分、精度3桁
    private static final Histogram HIST_US =
            new Histogram(1, java.util.concurrent.TimeUnit.MINUTES.toMicros(5), 3);

    // スレッド間共有: 短期ジッタ観測用（synchronizedで競合回避）
    private static final Histogram WINDOW_HIST_US =
        new Histogram(1, java.util.concurrent.TimeUnit.MINUTES.toMicros(5), 3);

    // 定数定義
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
    private static final Duration INITIAL_POLL_TIMEOUT = Duration.ofMillis(0);
    private static final long RESET_VALUE = 0L;
    private static final int SLEEP_MS_DURING_ASSIGNMENT_WAIT = 100;

    /**
     * 実行時状態を管理するクラス
     */
    private static class BenchmarkState {
        // 実行時集計変数
        long totalSeen = 0;              // 受信総数（ウォームアップ含む）
        long measured = 0;               // 計測対象のみ
        long firstNsMeasured = 0, lastNsMeasured = 0; // 計測期間（nanoTime使用）
        
        // Lag統計用（件数ベースのみで更新）
        long lagSamples = 0, lagSum = 0, lagMax = 0;
        long sinceLagSample = 0;
        
        // タイマー関連
        long lastCommitMs = System.currentTimeMillis();
        long lastLagLogMs = System.currentTimeMillis();
        long idleDeadline;
        
        // 設定値
        long commitIntervalMs;
        long lagSampleEveryMsgs;
        long lagLogIntervalMs;
        long expected;
        long idleMs;
        String runId;
        
        // 最終状態確認用（統計更新しない）
        long finalLag = -1;
        
        BenchmarkState(String runId, long expected, long idleMs, long commitIntervalMs, 
                      long lagSampleEveryMsgs, long lagLogIntervalMs) {
            this.runId = runId;
            this.expected = expected;
            this.idleMs = idleMs;
            this.commitIntervalMs = commitIntervalMs;
            this.lagSampleEveryMsgs = lagSampleEveryMsgs;
            this.lagLogIntervalMs = lagLogIntervalMs;
            this.idleDeadline = System.currentTimeMillis() + idleMs;
        }
    }

    /**
     * エラーハンドリングの統一
     */
    private static void handleNonCriticalError(String operation, Exception e) {
        logger.debug("{} failed: {}", operation, e.toString());
    }

    private static void handleCriticalError(String operation, Exception e) {
        logger.error("{} failed: {}", operation, e.toString());
    }

    /**
     * Lag計算の共通化関数
     */
    private static long calculateLag(KafkaConsumer<String, String> consumer) {
        try {
            var asn = consumer.assignment();
            if (asn.isEmpty()) return 0;
            
            var end = consumer.endOffsets(asn);
            long totalLag = 0;
            
            for (var tp : asn) {
                Long endOffset = end.get(tp);
                if (endOffset == null) {
                    logger.warn("Failed to get endOffset for partition {}", tp);
                    continue;
                }
                long pos = consumer.position(tp);
                totalLag += Math.max(0, endOffset - pos);
            }
            return totalLag;
        } catch (Exception e) {
            handleNonCriticalError("lag calculation", e);
            return 0;
        }
    }

    /**
     * メッセージ1件の処理
     */
    private static void processMessage(ConsumerRecord<String, String> rec, BenchmarkState state) {
        state.totalSeen++;
        boolean isWarm = false;

        // ペイロード解析 & E2E遅延記録
        try {
            JsonNode j = MAPPER.readTree(rec.value());
            isWarm = j.has("is_warmup") && j.get("is_warmup").asBoolean(false);

            if (!isWarm && j.has("ts_ns_send")) {
                recordLatency(j);
            } else if (!isWarm) {
                handleNonCriticalError("ts_ns_send field missing in payload", new IllegalStateException("missing field"));
            }
        } catch (Exception e) {
            handleNonCriticalError("payload parsing", e);
        }

        if (isWarm) return; // ウォームアップは計測対象外

        updateMeasurementCounters(state);
    }

    /**
     * 遅延測定とヒストグラム記録
     */
    private static void recordLatency(JsonNode j) {
        long tsSendNs = j.get("ts_ns_send").asLong();
        long nowNs = System.currentTimeMillis() * 1_000_000L;
        long latencyUs = Math.max(0, (nowNs - tsSendNs) / 1_000L);

        if (latencyUs <= HIST_US.getHighestTrackableValue()) {
            HIST_US.recordValue(latencyUs);
            synchronized (WINDOW_HIST_US) { 
                WINDOW_HIST_US.recordValue(latencyUs); 
            }
        }
    }

    /**
     * 計測カウンタの更新
     */
    private static void updateMeasurementCounters(BenchmarkState state) {
        if (state.firstNsMeasured == 0) state.firstNsMeasured = System.nanoTime();
        state.lastNsMeasured = System.nanoTime();
        
        state.measured++;
        messageCount.incrementAndGet();
        state.sinceLagSample++;
    }

    /**
     * 件数ベースLag処理（統計計算専用）
     */
    private static void handleCountBasedLag(KafkaConsumer<String, String> consumer, BenchmarkState state) {
        if (state.sinceLagSample < state.lagSampleEveryMsgs) return;
        
        long totalLag = calculateLag(consumer);
        
        // 統計のみ更新
        state.lagSamples++;
        state.lagSum += totalLag;
        state.lagMax = Math.max(state.lagMax, totalLag);
        
        logger.info("consumer_lag_total(sample-by-msgs)={}", totalLag);
        state.sinceLagSample = 0;
    }

    /**
     * 定期的なタスクの処理
     */
    private static void handlePeriodicTasks(KafkaConsumer<String, String> consumer, BenchmarkState state) {
        long nowMs = System.currentTimeMillis();
        
        handlePeriodicCommit(consumer, state, nowMs);
        handleTimeBasedLagLog(consumer, state, nowMs);
    }

    /**
     * 定期コミット処理
     */
    private static void handlePeriodicCommit(KafkaConsumer<String, String> consumer, BenchmarkState state, long nowMs) {
        if (nowMs - state.lastCommitMs < state.commitIntervalMs) return;
        
        OffsetCommitCallback onCommit = (offsets, ex) -> {
            if (ex != null) handleNonCriticalError("async commit", ex);
        };
        consumer.commitAsync(onCommit);
        state.lastCommitMs = nowMs;
    }

    /**
     * 時間ベースLag記録処理（可視化用記録専用）
     */
    private static void handleTimeBasedLagLog(KafkaConsumer<String, String> consumer, BenchmarkState state, long nowMs) {
        if (nowMs - state.lastLagLogMs < state.lagLogIntervalMs) return;
        
        long totalLag = calculateLag(consumer);
        
        // JSON記録のみ（統計更新しない）
        Map<String,Object> lagRow = new LinkedHashMap<>();
        lagRow.put("run_id", state.runId);
        lagRow.put("type", "lag");
        lagRow.put("ts_sec", Instant.now().getEpochSecond());
        lagRow.put("lag", totalLag);
        persistJsonLine(lagRow, state.runId);
        
        state.lastLagLogMs = nowMs;
    }

    /**
     * ポーリングとタイムアウトチェック
     */
    private static ConsumerRecords<String, String> pollWithTimeoutCheck(
            KafkaConsumer<String, String> consumer, BenchmarkState state) {
        var records = consumer.poll(POLL_TIMEOUT);

        if (records.isEmpty()) {
            long nowMs = System.currentTimeMillis();
            if (nowMs >= state.idleDeadline) {
                return null; // タイムアウト
            }
        } else {
            // メッセージ受信があったらデッドライン更新
            state.idleDeadline = System.currentTimeMillis() + state.idleMs;
        }
        
        return records;
    }

    /**
     * 終了条件の判定
     */
    private static boolean shouldTerminate(BenchmarkState state) {
        return state.expected > 0 && state.totalSeen >= state.expected;
    }

    /**
     * スケジューラタスクの設定
     */
    private static List<ScheduledFuture<?>> setupSchedulerTasks() {
        var tpsTask = scheduler.scheduleAtFixedRate(() -> {
            long tps = messageCount.getAndSet(RESET_VALUE);
            logger.info("1秒間のメッセージ受信数: {} 件", tps);
        }, 1, 1, TimeUnit.SECONDS);
        
        var winTask = scheduler.scheduleAtFixedRate(() -> {
            long w99;
            synchronized (WINDOW_HIST_US) {
                w99 = WINDOW_HIST_US.getValueAtPercentile(99.0);
                WINDOW_HIST_US.reset();
            }
            logger.info("window_p99_us={}", w99);
        }, 5, 5, TimeUnit.SECONDS);

        return Arrays.asList(tpsTask, winTask);
    }

    /**
     * Consumer設定の作成
     */
    private static Properties createConsumerConfig(String brokers, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return props;
    }

    /**
     * Consumer初期化処理
     */
    private static void initializeConsumer(KafkaConsumer<String, String> consumer, String topic) 
            throws InterruptedException {
        ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
            @Override public void onPartitionsRevoked(Collection<TopicPartition> parts) {
                logger.info("event=rebalance phase=revoked parts={}", parts);
                try { 
                    consumer.commitSync(); 
                } catch (Exception e) { 
                    handleNonCriticalError("sync commit on partition revoke", e);
                }
            }
            @Override public void onPartitionsAssigned(Collection<TopicPartition> parts) {
                boolean skipBacklog = Boolean.parseBoolean(System.getenv().getOrDefault("SKIP_BACKLOG","true"));
                if (skipBacklog) { consumer.pause(parts); consumer.seekToEnd(parts); consumer.resume(parts); }
                else { consumer.seekToBeginning(parts); }
                logger.info("assigned parts={}", parts);
            }
        };

        consumer.subscribe(Collections.singletonList(topic), listener);

        // 初期割り当て待ち
        consumer.poll(INITIAL_POLL_TIMEOUT);
        while (consumer.assignment().isEmpty()) {
            Thread.sleep(SLEEP_MS_DURING_ASSIGNMENT_WAIT);
            consumer.poll(INITIAL_POLL_TIMEOUT);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        // 環境変数から設定値取得
        String topic = System.getenv().getOrDefault("TOPIC",
                    System.getenv().getOrDefault("TOPIC_NAME", "events"));
        String brokers = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "kafka:9092");
        String groupId = System.getenv().getOrDefault("GROUP_ID", "bench-" + UUID.randomUUID());
        String runId  = System.getenv().getOrDefault("RUN_ID", String.valueOf(System.currentTimeMillis()));
        String profile = System.getenv().getOrDefault("PROFILE", "-");
        long expected = Long.parseLong(System.getenv().getOrDefault("EXPECTED_MSG", "0"));
        long idleMs   = Long.parseLong(System.getenv().getOrDefault("IDLE_MS", "1500"));
        long commitIntervalMs = Long.parseLong(System.getenv().getOrDefault("COMMIT_INTERVAL_MS", "1000"));
        long lagSampleEveryMsgs = Long.parseLong(System.getenv().getOrDefault("LAG_SAMPLE_EVERY_MSGS", "20000"));
        long lagLogIntervalMs = Long.parseLong(System.getenv().getOrDefault("LAG_LOG_INTERVAL_MS", "1000"));

        // 状態オブジェクト初期化
        BenchmarkState state = new BenchmarkState(runId, expected, idleMs, commitIntervalMs, 
                                                  lagSampleEveryMsgs, lagLogIntervalMs);

        Properties props = createConsumerConfig(brokers, groupId);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // シャットダウンフック
            final var closed = new java.util.concurrent.atomic.AtomicBoolean(false);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                closed.set(true);
                consumer.wakeup();
            }));

            initializeConsumer(consumer, topic);
            List<ScheduledFuture<?>> schedulerTasks = setupSchedulerTasks();

            // メインループ
            try {
                while (!closed.get()) {
                    var records = pollWithTimeoutCheck(consumer, state);
                    if (records == null) break; // タイムアウト
                    if (records.isEmpty()) continue;

                    // レコード処理
                    for (var rec : records) {
                        processMessage(rec, state);
                        handleCountBasedLag(consumer, state);
                    }

                    handlePeriodicTasks(consumer, state);
                    
                    if (shouldTerminate(state)) break;
                }
            } catch (org.apache.kafka.common.errors.WakeupException we) {
                logger.info("Wakeup received -> graceful shutdown");
            } finally {
                // 終了時最終Lag観測（最終状態確認専用、統計更新しない）
                state.finalLag = calculateLag(consumer);
                logger.info("final_consumer_lag_total={}", state.finalLag);

                // 最終コミット
                try { 
                    consumer.commitSync(); 
                } catch (Exception e) { 
                    handleNonCriticalError("final sync commit", e);
                }
            }

            // スケジューラタスク停止
            schedulerTasks.forEach(task -> task.cancel(false));

            // 結果出力
            outputResults(state, topic, groupId, profile);
        }

        scheduler.shutdown();
    }

    /**
     * 結果出力処理
     */
    private static void outputResults(BenchmarkState state, String topic, String groupId, String profile) {
        // スループット計算
        double elapsedSec = (state.lastNsMeasured > state.firstNsMeasured) ? 
                           (state.lastNsMeasured - state.firstNsMeasured) / 1e9 : 0.0;
        double mps = (elapsedSec > 0) ? (double) state.measured / elapsedSec : 0.0;

        logger.info("実行終了: total_seen={} measured={} warmup_ignored={} 経過秒(測定)~{} 平均MPS(測定)~{}",
                state.totalSeen, state.measured, (state.totalSeen - state.measured),
                String.format("%.3f", elapsedSec), String.format("%.0f", mps));

        // 分位値計算
        long p50 = HIST_US.getValueAtPercentile(50.0);
        long p95 = HIST_US.getValueAtPercentile(95.0);
        long p99 = HIST_US.getValueAtPercentile(99.0);
        long p999 = HIST_US.getValueAtPercentile(99.9);
        long hdrCount = HIST_US.getTotalCount();
        double tailRatio = (p50 > 0) ? (double)p99 / (double)p50 : 0.0;
        double tailStep999 = (p99 > 0) ? (double)p999 / (double)p99 : 0.0;

        logger.info("latency_us p50={}, p95={}, p99={}, p99.9={}", p50, p95, p99, p999);

        // Producer設定取得
        String acks = System.getenv().getOrDefault("ACKS", "all");
        String lingerMs = System.getenv().getOrDefault("LINGER",
                            System.getenv().getOrDefault("LINGER_MS", "0"));
        String batchSize = System.getenv().getOrDefault("BATCH",
                            System.getenv().getOrDefault("BATCH_SIZE", "16384"));

        // 結果JSON作成
        var result = new LinkedHashMap<String, Object>();
        addBasicInfo(result, state.runId, profile);
        addKafkaConfig(result, topic, groupId, acks, lingerMs, batchSize, state.expected, state.idleMs, state.commitIntervalMs);
        addLatencyMetrics(result, p50, p95, p99, p999, hdrCount, tailRatio, tailStep999);
        addThroughputMetrics(result, state.totalSeen, state.measured, elapsedSec, mps);
        addLagMetrics(result, state.lagSamples, state.lagSum, state.lagMax, state.finalLag, state.lagSampleEveryMsgs);

        persistJsonLine(result, state.runId);

        System.out.printf("RESULT,acks=%s,linger=%s,batch=%s,p50=%d,p95=%d,p99=%d,received=%d%n",
                acks, lingerMs, batchSize, p50, p95, p99, state.totalSeen);
    }

    /**
     * 基本情報の追加
     */
    private static void addBasicInfo(Map<String, Object> result, String runId, String profile) {
        result.put("project", "event-switchyard");
        result.put("git_rev", System.getenv().getOrDefault("GIT_REV", "unknown"));
        result.put("ts", Instant.now().toString());
        result.put("run_id", runId);
        result.put("profile", profile);
    }

    /**
     * Kafka設定の追加
     */
    private static void addKafkaConfig(Map<String, Object> result, String topic, String groupId, 
                                       String acks, String lingerMs, String batchSize, 
                                       long expected, long idleMs, long commitIntervalMs) {
        result.put("topic", topic);
        result.put("group_id", groupId);
        result.put("acks", acks);
        result.put("linger_ms", Integer.parseInt(lingerMs));
        result.put("batch_size", Integer.parseInt(batchSize));
        result.put("expected", expected);
        result.put("idle_ms", idleMs);
        result.put("commit_interval_ms", commitIntervalMs);
        
        String keyStrategy = System.getenv().getOrDefault("KEY_STRATEGY", "none");
        result.put("key_strategy", keyStrategy);
    }

    /**
     * 遅延メトリクスの追加
     */
    private static void addLatencyMetrics(Map<String, Object> result, long p50, long p95, long p99, long p999,
                                          long hdrCount, double tailRatio, double tailStep999) {
        result.put("p50_us", p50);
        result.put("p95_us", p95);
        result.put("p99_us", p99);
        result.put("p999_us", p999);
        result.put("hdr_count", hdrCount);
        result.put("tail_ratio", tailRatio);
        result.put("tail_step_999", tailStep999);
    }

    /**
     * スループットメトリクスの追加
     */
    private static void addThroughputMetrics(Map<String, Object> result, long totalSeen, long measured, 
                                             double elapsedSec, double mps) {
        result.put("received", totalSeen);
        result.put("measured", measured);
        result.put("elapsed_sec", elapsedSec);
        result.put("throughput_mps", mps);
        result.put("warmup_ignored", totalSeen - measured);
    }

    /**
     * Lagメトリクスの追加
     */
    private static void addLagMetrics(Map<String, Object> result, long lagSamples, long lagSum, 
                                      long lagMax, long finalLag, long lagSampleEveryMsgs) {
        result.put("lag_samples", lagSamples);
        double lagAvg = (lagSamples > 0) ? (double) lagSum / (double) lagSamples : 0.0;
        result.put("lag_avg", lagAvg);
        result.put("lag_max", lagMax);
        result.put("lag_final", Math.max(0, finalLag));
        result.put("lag_sample_every_msgs", lagSampleEveryMsgs);
    }

    /**
     * JSON安全追記: FileChannel + lock()で排他、force(true)でfsync
     */
    static void persistJsonLine(Map<String, Object> obj, String runId) {
        try {
            Path dir = Paths.get(System.getenv().getOrDefault("RESULTS_DIR", "/var/log/results"));
            Files.createDirectories(dir);
            Path file = dir.resolve("bench-" + runId + ".jsonl");
            String line = MAPPER.writeValueAsString(obj) + "\n";
            try (FileChannel ch = FileChannel.open(file, EnumSet.of(
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND))) {
                try (var lock = ch.lock()) {
                    ch.write(ByteBuffer.wrap(line.getBytes(StandardCharsets.UTF_8)));
                    ch.force(true); // fsync
                }
            }
        } catch (Exception e) {
            handleCriticalError("result persistence", e);
        }
    }
}