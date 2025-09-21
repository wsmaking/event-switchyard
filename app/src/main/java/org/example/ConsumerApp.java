package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.HdrHistogram.Histogram;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 計測付きKafkaコンシューマ: E2E遅延をHDR Histogramに記録（p50/p95/p99/p99.9）
 * 
 * スレッド構成:
 * - メインスレッド: Kafkaメッセージ処理
 * - schedulerスレッド: 定期監視（TPS + ウィンドウp99）
 * - shutdown hookスレッド: 安全終了処理
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

        // 実行時集計変数
        long totalSeen = 0;              // 受信総数（ウォームアップ含む）
        long measured = 0;               // 計測対象のみ
        long firstNsMeasured = 0, lastNsMeasured = 0; // 計測期間（nanoTime使用）
        long lagSamples = 0, lagSum = 0, lagMax = 0;

        // Consumer設定
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // シャットダウンフック: 安全終了用
            final var closed = new java.util.concurrent.atomic.AtomicBoolean(false);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                closed.set(true);
                consumer.wakeup();
            }));

            // Rebalanceハンドラ
            ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
                @Override public void onPartitionsRevoked(Collection<TopicPartition> parts) {
                    logger.info("event=rebalance phase=revoked parts={}", parts);
                    try { consumer.commitSync(); } catch (Exception e) { logger.warn("commitSync on revoke failed", e); }
                }
                @Override public void onPartitionsAssigned(Collection<TopicPartition> parts) {
                    boolean skipBacklog = Boolean.parseBoolean(System.getenv().getOrDefault("SKIP_BACKLOG","true"));
                    if (skipBacklog) { consumer.pause(parts); consumer.seekToEnd(parts); consumer.resume(parts); }
                    else { consumer.seekToBeginning(parts); }
                    logger.info("assigned parts={}", parts);
                }
            };

            consumer.subscribe(Collections.singletonList(topic), listener);

            // 初期割り当て待ち: poll(0)で即時返し
            consumer.poll(Duration.ofMillis(0));
            while (consumer.assignment().isEmpty()) {
                Thread.sleep(100);
                consumer.poll(Duration.ofMillis(0));
            }

            // schedulerスレッド: TPS監視（1秒間隔）
            var tpsTask = scheduler.scheduleAtFixedRate(() -> {
                long tps = messageCount.getAndSet(0); // AtomicLong: 安全な読み取り&リセット
                logger.info("1秒間のメッセージ受信数: {} 件", tps);
            }, 1, 1, TimeUnit.SECONDS);
            
            // schedulerスレッド: ウィンドウp99監視（5秒間隔）
            var winTask = scheduler.scheduleAtFixedRate(() -> {
                long w99;
                synchronized (WINDOW_HIST_US) { // 排他制御: 読み取り中のリセット防止
                    w99 = WINDOW_HIST_US.getValueAtPercentile(99.0);
                    WINDOW_HIST_US.reset();
                }
                logger.info("window_p99_us={}", w99);
            }, 5, 5, TimeUnit.SECONDS);

            OffsetCommitCallback onCommit = (offsets, ex) -> {
                if (ex != null) logger.warn("commitAsync failed: {}", ex.toString());
            };

            long idleDeadline = System.currentTimeMillis() + idleMs;

            // タイマ関連設定
            long lastCommitMs = System.currentTimeMillis();
            long commitIntervalMs = Long.parseLong(System.getenv().getOrDefault("COMMIT_INTERVAL_MS", "1000"));
            long lagSampleEveryMsgs = Long.parseLong(System.getenv().getOrDefault("LAG_SAMPLE_EVERY_MSGS", "20000"));
            long sinceLagSample = 0;
            long lastLagObserved = -1;
            long lagLogIntervalMs = Long.parseLong(System.getenv().getOrDefault("LAG_LOG_INTERVAL_MS", "1000"));
            long lastLagLogMs = System.currentTimeMillis();

            // メインループ
            try {
                while (!closed.get()) {
                    var records = consumer.poll(Duration.ofMillis(100));

                    if (records.isEmpty()) {
                        long nowMs = System.currentTimeMillis();
                        if (nowMs >= idleDeadline) break;
                        continue;
                    }
                    idleDeadline = System.currentTimeMillis() + idleMs;

                    for (var rec : records) {
                        totalSeen++;
                        boolean isWarm = false;

                        // ペイロード解析 & E2E遅延記録
                        try {
                            JsonNode j = MAPPER.readTree(rec.value());
                            isWarm = j.has("is_warmup") && j.get("is_warmup").asBoolean(false);

                            if (!isWarm && j.has("ts_ns_send")) {
                                // E2E遅延計算: Producer送信時刻 → Consumer受信時刻
                                long tsSendNs = j.get("ts_ns_send").asLong();
                                long nowNs    = System.currentTimeMillis() * 1_000_000L; // currentTimeMillis: 異なるマシン間で時刻共有
                                long latencyUs = Math.max(0, (nowNs - tsSendNs) / 1_000L);

                                if (latencyUs <= HIST_US.getHighestTrackableValue()) {
                                    HIST_US.recordValue(latencyUs);
                                    synchronized (WINDOW_HIST_US) { WINDOW_HIST_US.recordValue(latencyUs); } // synchronizedで競合回避
                                }
                            } else if (!isWarm) {
                                logger.debug("ts_ns_send not found in payload");
                            }
                        } catch (Exception e) {
                            logger.debug("payload parse failed: {}", e.toString());
                        }

                        if (isWarm) {
                            continue; // ウォームアップは計測対象外
                        }

                        // 計測対象の集計
                        if (firstNsMeasured == 0) firstNsMeasured = System.nanoTime(); // nanoTime: 期間測定用（時刻調整に影響されない）
                        lastNsMeasured = System.nanoTime();

                        measured++;
                        messageCount.incrementAndGet(); // AtomicLong: メインスレッドからのインクリメント
                        sinceLagSample++;

                        // 件数ベースLagサンプリング
                        if (sinceLagSample >= lagSampleEveryMsgs) {
                            try {
                                var asn = consumer.assignment();
                                if (!asn.isEmpty()) {
                                    var end = consumer.endOffsets(asn);
                                    long totalLag = 0;
                                    for (var tp : asn) {
                                        long pos = consumer.position(tp);
                                        totalLag += Math.max(0, end.getOrDefault(tp, pos) - pos);
                                    }
                                    lagSamples++;
                                    lagSum += totalLag;
                                    lagMax = Math.max(lagMax, totalLag);
                                    lastLagObserved = totalLag;
                                    logger.info("consumer_lag_total(sample-by-msgs)={}", totalLag);
                                }
                            } catch (Exception e) {
                                logger.debug("lag calc (by msgs) skipped: {}", e.toString());
                            }
                            sinceLagSample = 0;
                        }
                    }

                    // 定期コミット（非同期）
                    if (System.currentTimeMillis() - lastCommitMs >= commitIntervalMs) {
                        consumer.commitAsync(onCommit);
                        lastCommitMs = System.currentTimeMillis();
                    }

                    // 時間ベースLagログ（1秒粒度）
                    long nowMs = System.currentTimeMillis();
                    if (nowMs - lastLagLogMs >= lagLogIntervalMs) {
                        try {
                            var asn = consumer.assignment();
                            if (!asn.isEmpty()) {
                                var end = consumer.endOffsets(asn);
                                long totalLag = 0;
                                for (var tp : asn) {
                                    long pos = consumer.position(tp);
                                    totalLag += Math.max(0, end.getOrDefault(tp, pos) - pos);
                                }
                                Map<String,Object> lagRow = new LinkedHashMap<>();
                                lagRow.put("run_id", runId);
                                lagRow.put("type", "lag");
                                lagRow.put("ts_sec", Instant.now().getEpochSecond());
                                lagRow.put("lag", totalLag);
                                persistJsonLine(lagRow, runId);
                            }
                        } catch (Exception ignore) { /* 観測失敗は無視 */ }
                        lastLagLogMs = nowMs;
                    }

                    if (expected > 0 && totalSeen >= expected) break;

                }
            } catch (org.apache.kafka.common.errors.WakeupException we) {
                logger.info("Wakeup received -> graceful shutdown");
            } finally {
                // 終了時最終Lag観測
                try {
                    var asn = consumer.assignment();
                    if (!asn.isEmpty()) {
                        var end = consumer.endOffsets(asn);
                        long totalLag = 0;
                        for (var tp : asn) {
                            long pos = consumer.position(tp);
                            totalLag += Math.max(0, end.getOrDefault(tp, pos) - pos);
                        }
                        lagSamples++;
                        lagSum += totalLag;
                        lagMax = Math.max(lagMax, totalLag);
                        lastLagObserved = totalLag;
                        logger.info("final_consumer_lag_total={}", totalLag);
                    }
                } catch (Exception e) {
                    logger.debug("final lag calc skipped: {}", e.toString());
                }

                // 最終コミット（同期）
                try { consumer.commitSync(); } catch (Exception e) { logger.warn("final commit failed", e); }
            }

            // 集計 & 永続化
            tpsTask.cancel(false);
            winTask.cancel(false);

            // スループット計算: nanoTime使用（期間測定）
            double elapsedSec = (lastNsMeasured > firstNsMeasured) ? (lastNsMeasured - firstNsMeasured) / 1e9 : 0.0;
            double mps = (elapsedSec > 0) ? (double) measured / elapsedSec : 0.0;

            logger.info("実行終了: total_seen={} measured={} warmup_ignored={} 経過秒(測定)~{} 平均MPS(測定)~{}",
                    totalSeen, measured, (totalSeen - measured),
                    String.format("%.3f", elapsedSec),
                    String.format("%.0f", mps));

            // 分位値計算
            long p50 = HIST_US.getValueAtPercentile(50.0);
            long p95 = HIST_US.getValueAtPercentile(95.0);
            long p99 = HIST_US.getValueAtPercentile(99.0);
            long p999 = HIST_US.getValueAtPercentile(99.9);
            long hdrCount = HIST_US.getTotalCount();
            double tailRatio = (p50 > 0) ? (double)p99 / (double)p50 : 0.0;   // テールの重さ
            double tailStep999 = (p99 > 0) ? (double)p999 / (double)p99 : 0.0; // 超テール

            logger.info("latency_us p50={}, p95={}, p99={}, p99.9={}", p50, p95, p99, p999);

            // Producer設定取得
            String acks      = System.getenv().getOrDefault("ACKS", "all");
            String lingerMs  = System.getenv().getOrDefault("LINGER",
                                System.getenv().getOrDefault("LINGER_MS", "0"));
            String batchSize = System.getenv().getOrDefault("BATCH",
                                System.getenv().getOrDefault("BATCH_SIZE", "16384"));

            // 結果JSON作成
            var result = new LinkedHashMap<String, Object>();
            result.put("project", "event-switchyard");
            result.put("git_rev", System.getenv().getOrDefault("GIT_REV", "unknown"));
            result.put("ts", Instant.now().toString());
            result.put("run_id", runId);
            result.put("profile", profile);
            result.put("topic", topic);
            result.put("group_id", groupId);
            result.put("acks", acks);
            result.put("linger_ms", Integer.parseInt(lingerMs));
            result.put("batch_size", Integer.parseInt(batchSize));
            result.put("expected", expected);
            result.put("idle_ms", idleMs);
            result.put("p50_us", p50);
            result.put("p95_us", p95);
            result.put("p99_us", p99);
            result.put("p999_us", p999);
            result.put("hdr_count", hdrCount);
            result.put("tail_ratio", tailRatio);
            result.put("tail_step_999", tailStep999);
            result.put("received", totalSeen);
            result.put("measured", measured);
            result.put("elapsed_sec", elapsedSec);
            result.put("throughput_mps", mps);
            result.put("commit_interval_ms", commitIntervalMs);
            result.put("warmup_ignored", totalSeen - measured);
            result.put("lag_samples", lagSamples);
            result.put("lag_final", Math.max(0, lastLagObserved));

            double lagAvg = (lagSamples > 0) ? (double) lagSum / (double) lagSamples : 0.0;
            result.put("lag_avg", lagAvg);
            result.put("lag_max", lagMax);
            result.put("lag_sample_every_msgs", lagSampleEveryMsgs);

            String keyStrategy = System.getenv().getOrDefault("KEY_STRATEGY", "none");
            result.put("key_strategy", keyStrategy);

            persistJsonLine(result, runId);

            System.out.printf(
                "RESULT,acks=%s,linger=%s,batch=%s,p50=%d,p95=%d,p99=%d,received=%d%n",
                acks, lingerMs, batchSize, p50, p95, p99, totalSeen
            );
        }

        scheduler.shutdown();
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
            logger.error("failed to persist result", e);
        }
    }
}