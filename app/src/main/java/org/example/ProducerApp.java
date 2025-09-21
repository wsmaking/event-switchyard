package org.example;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.Properties;
import java.util.UUID;

import org.HdrHistogram.Recorder;
import org.HdrHistogram.Histogram;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerApp {

    // JSON直列化（軽量ペイロード生成）
    private static final com.fasterxml.jackson.databind.ObjectMapper OM =
        new com.fasterxml.jackson.databind.ObjectMapper();

    public static void main(String[] args) throws Exception {
        // ====== 入力（環境変数） ======
        String brokers   = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "kafka:9092");
        String acks      = System.getenv().getOrDefault("ACKS", "all"); // 0|1|all
        boolean idem     = !"1".equals(acks); // acks=1なら冪等送信OFF
        String topic     = System.getenv().getOrDefault("TOPIC_NAME", "events");
        int lingerMs     = Integer.parseInt(System.getenv().getOrDefault("LINGER_MS", "0"));
        int batchSize    = Integer.parseInt(System.getenv().getOrDefault("BATCH_SIZE", "16384"));
        long numMsg      = Long.parseLong(System.getenv().getOrDefault("NUM_MSG", "0")); // 0=時間制
        int payloadBytes = Integer.parseInt(System.getenv().getOrDefault("PAYLOAD_BYTES", "200"));
        long warmupMsgs  = Long.parseLong(System.getenv().getOrDefault("WARMUP_MSGS", "0"));
        String producerId= System.getenv().getOrDefault("HOSTNAME", UUID.randomUUID().toString());
        String keyStrategy = System.getenv().getOrDefault("KEY_STRATEGY", "none"); // none|symbol|order_id
        int HOT_KEY_EVERY = Integer.parseInt(System.getenv().getOrDefault("HOT_KEY_EVERY", "0"));
        String runId      = System.getenv().getOrDefault("RUN_ID", producerId);

        String[] symbols = {"AAPL","MSFT","AMZN","NVDA","GOOGL","META","TSLA","ORCL","AVGO","AMD"};
        java.util.Random rnd = new java.util.Random(7);

        // 固定長パディング（メッセージサイズ安定化）
        char[] padChars = new char[payloadBytes];
        java.util.Arrays.fill(padChars, 'x');
        final String pad = new String(padChars);

        // 非同期送信エラー件数
        java.util.concurrent.atomic.AtomicLong sendErrors = new java.util.concurrent.atomic.AtomicLong();

        // ====== Producer 設定 ======
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idem);
        if (!idem) {
            // スループット寄せ（順序崩れ許容）
            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        }
        props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-" + UUID.randomUUID());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
                System.getenv().getOrDefault("COMPRESSION_TYPE","none")); // none|lz4|zstd

        // ====== ACKレイテンシ計測（μs） ======
        boolean ackRecorderOn = "1".equals(System.getenv().getOrDefault("ACK_RECORDER","1"));
        String resultsDir     = System.getenv().getOrDefault("RESULTS_DIR",".");
        String ackHgrmPath    = System.getenv().getOrDefault("ACK_HIST_PATH",
                                resultsDir + "/ack-" + runId + ".hgrm");
        Recorder ackRec = ackRecorderOn ? new Recorder(1, 60_000_000L, 3) : null; // 1µs〜60s, 3桁

        // 時間制実行（NUM_MSG=0のとき有効）
        long runSecs = Long.parseLong(System.getenv().getOrDefault("RUN_SECS","30"));

        // ====== メインループ ======
        try (KafkaProducer<String,String> producer = new KafkaProducer<>(props)) {
            long i = 0;
            long endNs = System.nanoTime() + Duration.ofSeconds(runSecs).toNanos();

            while ((numMsg == 0 && System.nanoTime() < endNs) || (numMsg > 0 && i < numMsg)) {
                String symbol  = symbols[rnd.nextInt(symbols.length)];
                String orderId = "o-" + UUID.randomUUID();

                // キー戦略（分散/ホットキー制御）
                String key = switch (keyStrategy) {
                    case "symbol"   -> symbol;
                    case "order_id" -> orderId;
                    default -> (HOT_KEY_EVERY > 0 && (i % HOT_KEY_EVERY) == 0) ? "HOT" : "k-" + UUID.randomUUID();
                };

                // 送信時刻（ns, wall clock）
                long tsSendNs = System.currentTimeMillis() * 1_000_000L;

                // ペイロード（最小限の注文イベント想定）
                var node = OM.createObjectNode()
                    .put("order_id", orderId)
                    .put("symbol", symbol)
                    .put("side", rnd.nextBoolean() ? "BUY" : "SELL")
                    .put("qty", 100)
                    .put("price", 123456)
                    .put("ts_ns_send", tsSendNs)
                    .put("correlation_id", runId)
                    .put("seq", i)
                    .put("producer_id", producerId)
                    .put("is_warmup", i < warmupMsgs)
                    .put("pad", pad);

                String payload = node.toString();

                // ウォームアップ外のみ計測
                final boolean warm = i >= warmupMsgs;
                final long t0 = System.nanoTime();

                // 非同期送信＋ACKコールバック
                producer.send(new ProducerRecord<>(topic, key, payload), (md, ex) -> {
                    if (ex != null) { sendErrors.incrementAndGet(); return; }
                    if (ackRecorderOn && warm) {
                        long us = Math.max(0, (System.nanoTime() - t0) / 1000);
                        ackRec.recordValue(us);
                    }
                });
                i++;
            }

            // 送信完了（残キュー排出）
            producer.flush();
            System.out.println("送信完了: 件数=" + i + " send_errors=" + sendErrors.get());

            // ====== 結果出力（.hgrm + 要約JSONL） ======
            if (ackRecorderOn) {
                Histogram h = ackRec.getIntervalHistogram();
                Files.createDirectories(Paths.get(resultsDir));
                try (PrintStream ps = new PrintStream(
                        new BufferedOutputStream(new FileOutputStream(ackHgrmPath)),
                        false, StandardCharsets.UTF_8.name())) {
                    h.outputPercentileDistribution(ps, 1.0); // μs
                }
                String summary = String.format(
                  "{\"run_id\":\"%s\",\"acks\":\"%s\",\"linger_ms\":%d,\"batch_size\":%d," +
                  "\"ack_p50_us\":%d,\"ack_p95_us\":%d,\"ack_p99_us\":%d,\"ack_count\":%d}\n",
                  runId, acks, lingerMs, batchSize,
                  (long)h.getValueAtPercentile(50), (long)h.getValueAtPercentile(95),
                  (long)h.getValueAtPercentile(99), h.getTotalCount());
                Files.writeString(Paths.get(resultsDir, "ack-summary.jsonl"), summary,
                                  StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
        }
    }
}
