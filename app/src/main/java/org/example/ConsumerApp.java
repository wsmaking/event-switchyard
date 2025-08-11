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

public class ConsumerApp {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerApp.class);

    private static final AtomicLong messageCount = new AtomicLong(0);
    private static final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Histogram HIST_US =
            new Histogram(1, java.util.concurrent.TimeUnit.MINUTES.toMicros(5), 3);

    public static void main(String[] args) throws InterruptedException {
        // ====== 入力（環境変数） ======
        String topic   = System.getenv().getOrDefault("TOPIC_NAME", "events");
        String brokers = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
        String groupId = System.getenv().getOrDefault("GROUP_ID", "bench-" + UUID.randomUUID());

        long expected = Long.parseLong(System.getenv().getOrDefault("EXPECTED_MSG", "0")); // 0=無効
        long idleMs   = Long.parseLong(System.getenv().getOrDefault("IDLE_MS", "1500"));

        // ====== Consumer 設定 ======
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // シャットダウンフック（OK）
            final var closed = new java.util.concurrent.atomic.AtomicBoolean(false);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                closed.set(true);
                consumer.wakeup();
            }));

            // rebalance（OK）
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

            // 初期割り当て待ち（OK）
            consumer.poll(Duration.ofMillis(0));
            while (consumer.assignment().isEmpty()) {
                Thread.sleep(100);
                consumer.poll(Duration.ofMillis(0));
            }

            var tpsTask = scheduler.scheduleAtFixedRate(() -> {
                long tps = messageCount.getAndSet(0);
                logger.info("1秒間のメッセージ受信数: {} 件", tps);
            }, 1, 1, TimeUnit.SECONDS);

            // ★ lagログ用のタイムスタンプ
            long lastLagLogMs = System.currentTimeMillis();

            OffsetCommitCallback onCommit = (offsets, ex) -> {
                if (ex != null) logger.warn("commitAsync failed: {}", ex.toString());
            };

            long received = 0;
            long firstNs = 0, lastNs = 0;
            long idleDeadline = System.currentTimeMillis() + idleMs;

            // ★ 追加: 定期コミット用タイマ
            long lastCommitMs = System.currentTimeMillis();
            long commitIntervalMs = Long.parseLong(System.getenv().getOrDefault("COMMIT_INTERVAL_MS", "1000"));

            // ====== メインループ ======
            try { // ★ WakeupException を吸収して正常クローズへ
                while (!closed.get()) { // ★ closed を見る
                    var records = consumer.poll(Duration.ofMillis(100));

                    if (records.isEmpty()) {
                        long nowMs = System.currentTimeMillis();
                        if (nowMs - lastLagLogMs >= 1000) {
                            try {
                                var asn = consumer.assignment();
                                if (!asn.isEmpty()) {
                                    var end = consumer.endOffsets(asn);
                                    long totalLag = 0;
                                    for (var tp : asn) {
                                        long pos = consumer.position(tp);
                                        totalLag += Math.max(0, end.getOrDefault(tp, pos) - pos);
                                    }
                                    logger.info("consumer_lag_total={}", totalLag);
                                }
                            } catch (Exception e) {
                                logger.debug("lag calc skipped: {}", e.toString());
                            }
                            lastLagLogMs = nowMs;
                        }

                        if (nowMs - lastCommitMs >= commitIntervalMs) {
                            consumer.commitAsync(onCommit);
                            lastCommitMs = nowMs;
                        }
                        if (nowMs >= idleDeadline) break;
                        continue;
                    }
                    idleDeadline = System.currentTimeMillis() + idleMs;

                    for (var rec : records) {
                        if (firstNs == 0) firstNs = System.nanoTime();
                        lastNs = System.nanoTime();

                        received++;
                        messageCount.incrementAndGet();

                        try {
                            JsonNode j = MAPPER.readTree(rec.value());
                            if (j.has("ts_ns")) {
                                long tsSendNs = j.get("ts_ns").asLong();
                                long nowNs = System.currentTimeMillis() * 1_000_000L;
                                long latencyUs = Math.max(0, (nowNs - tsSendNs) / 1_000L);
                                if (latencyUs <= HIST_US.getHighestTrackableValue()) {
                                    HIST_US.recordValue(latencyUs);
                                } else {
                                    logger.debug("skip latency={}us over histogram range", latencyUs);
                                }
                            } else {
                                logger.debug("ts_ns not found in payload");
                            }
                        } catch (Exception e) {
                            logger.warn("payload parse failed: {}", e.toString());
                        }
                    }

                    // ★ 定期コミット（軽量）
                    if (System.currentTimeMillis() - lastCommitMs >= commitIntervalMs) {
                        consumer.commitAsync();
                        lastCommitMs = System.currentTimeMillis();
                    }

                    if (expected > 0 && received >= expected) break;

                    // ★ 1秒おきにラグを計算（同一スレッドで安全に）
                    long nowMs = System.currentTimeMillis();
                    if (nowMs - lastLagLogMs >= 1000) {
                        try {
                            var asn = consumer.assignment();
                            if (!asn.isEmpty()) {
                                var end = consumer.endOffsets(asn);
                                long totalLag = 0;
                                for (var tp : asn) {
                                    long pos = consumer.position(tp);
                                    totalLag += Math.max(0, end.getOrDefault(tp, pos) - pos);
                                }
                                logger.info("consumer_lag_total={}", totalLag);
                            }
                        } catch (Exception e) {
                            logger.debug("lag calc skipped: {}", e.toString());
                        }
                        lastLagLogMs = nowMs;
                    }
                }
            } catch (org.apache.kafka.common.errors.WakeupException we) {
                // ★ shutdown 用なので握りつぶし
                logger.info("Wakeup received -> graceful shutdown");
            } finally {
                // ★ 最終コミット（同期）
                try { consumer.commitSync(); } catch (Exception e) { logger.warn("final commit failed", e); }
            }

            // ====== 集計 & 永続化 ======
            tpsTask.cancel(false);

            double elapsedSec = (lastNs > firstNs) ? (lastNs - firstNs) / 1e9 : 0.0;
            double mps = (elapsedSec > 0) ? received / elapsedSec : 0.0;
            logger.info("実行終了: 受信件数={} 経過秒~{} 平均MPS~{}",
                    received,
                    String.format("%.3f", elapsedSec),
                    String.format("%.0f", mps));

            long p50 = HIST_US.getValueAtPercentile(50.0);
            long p95 = HIST_US.getValueAtPercentile(95.0);
            long p99 = HIST_US.getValueAtPercentile(99.0);
            logger.info("latency_us p50={}, p95={}, p99={}", p50, p95, p99);

            String runId     = System.getenv().getOrDefault("RUN_ID", String.valueOf(System.currentTimeMillis()));
            String acks      = System.getenv().getOrDefault("ACKS", "all");
            String lingerMs  = System.getenv().getOrDefault("LINGER_MS", "0");
            String batchSize = System.getenv().getOrDefault("BATCH_SIZE", "16384");

            var result = new LinkedHashMap<String, Object>();
            result.put("project", "event-switchyard");
            result.put("git_rev", System.getenv().getOrDefault("GIT_REV", "unknown"));
            result.put("ts", Instant.now().toString());
            result.put("run_id", runId);
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
            result.put("received", received);
            result.put("elapsed_sec", elapsedSec);
            result.put("throughput_mps", mps);
            result.put("commit_interval_ms", commitIntervalMs);

            persistJsonLine(result, runId);

            // bench.sh が読む旧フォーマット（CSV風ログ）は維持
            System.out.printf(
                    "RESULT,acks=%s,linger=%s,batch=%s,p50=%d,p95=%d,p99=%d,received=%d%n",
                    acks, lingerMs, batchSize, p50, p95, p99, received
            );
        }

        scheduler.shutdown();
    }

    static void persistJsonLine(Map<String, Object> obj, String runId) {
        try {
            Path dir = Paths.get(System.getenv().getOrDefault("RESULTS_DIR", "/var/log/results"));
            Files.createDirectories(dir);
            Path file = dir.resolve("bench-" + runId + ".jsonl");
            String line = MAPPER.writeValueAsString(obj) + "\n";
            try (FileChannel ch = FileChannel.open(file, EnumSet.of(
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND))) {
                try (var lock = ch.lock()) { // 排他
                    ch.write(ByteBuffer.wrap(line.getBytes(StandardCharsets.UTF_8)));
                    ch.force(true); // fsync（内容+メタデータ）
                }
            }
        } catch (Exception e) {
            logger.error("failed to persist result", e);
        }
    }
}
