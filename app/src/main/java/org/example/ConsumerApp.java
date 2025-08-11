package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.HdrHistogram.Histogram;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
import java.util.stream.Collectors;

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
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        ConsumerRebalanceListener rebalanceListener = new ConsumerRebalanceListener() {
            private final ObjectMapper mapper = new ObjectMapper();

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                try {
                    var list = partitions.stream()
                            .map(tp -> Map.of("topic", tp.topic(), "partition", tp.partition()))
                            .collect(Collectors.toList());
                    String json = mapper.writeValueAsString(list);
                    logger.info("event=rebalance phase=revoked partitions={}", json);
                } catch (Exception e) {
                    logger.error("パーティション情報の変換に失敗: {}", e.getMessage());
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                try {
                    var list = partitions.stream()
                            .map(tp -> Map.of("topic", tp.topic(), "partition", tp.partition()))
                            .collect(Collectors.toList());
                    String json = mapper.writeValueAsString(list);
                    logger.info("event=rebalance phase=assigned partitions={}", json);
                } catch (Exception e) {
                    logger.warn("JSON 変換失敗", e);
                }
            }
        };

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic), rebalanceListener);

            // 初期パーティション割り当て待ち
            consumer.poll(Duration.ofMillis(0));
            while (consumer.assignment().isEmpty()) {
                Thread.sleep(100);
                consumer.poll(Duration.ofMillis(0));
            }
            // 最初から読む
            consumer.seekToBeginning(consumer.assignment());

            // TPS ロガー（毎秒）
            var tpsTask = scheduler.scheduleAtFixedRate(() -> {
                long tps = messageCount.getAndSet(0);
                logger.info("1秒間のメッセージ受信数: {} 件", tps);
            }, 1, 1, TimeUnit.SECONDS);

            long received = 0;
            long firstNs = 0, lastNs = 0;
            long idleDeadline = System.currentTimeMillis() + idleMs;

            // ====== メインループ：件数達成 or アイドルで終了 ======
            while (true) {
                var records = consumer.poll(Duration.ofMillis(100));

                if (records.isEmpty()) {
                    if (System.currentTimeMillis() >= idleDeadline) break; // アイドル終了
                    continue;
                }
                idleDeadline = System.currentTimeMillis() + idleMs;

                for (var rec : records) {
                    if (firstNs == 0) firstNs = System.nanoTime();
                    lastNs = System.nanoTime();

                    received++;
                    messageCount.incrementAndGet();

                    try {
                        JsonNode j = MAPPER.readTree(rec.value()); // {"...","ts_ns":<long>}
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

                if (expected > 0 && received >= expected) {
                    break; // 目標件数に達したら終了
                }
            }

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

            // ====== 結果永続化 ======
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

            persistJsonLine(result, runId);

            // bench.sh が読む旧フォーマット（CSV風ログ）は維持
            System.out.printf(
                    "RESULT,acks=%s,linger=%s,batch=%s,p50=%d,p95=%d,p99=%d,received=%d%n",
                    acks, lingerMs, batchSize, p50, p95, p99, received
            );
        }

        // 最後のTPSログをキャッチしてから終了
        Thread.sleep(1100);
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
