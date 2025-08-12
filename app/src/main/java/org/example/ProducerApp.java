// ProducerApp.java（主要部）
package org.example;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerApp {

    private static final com.fasterxml.jackson.databind.ObjectMapper OM =
        new com.fasterxml.jackson.databind.ObjectMapper();

    public static void main(String[] args) throws Exception {
        String brokers   = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "kafka:9092");
        String acks      = System.getenv().getOrDefault("ACKS", "all");       
        boolean idem = !"1".equals(acks);
        String topic   = System.getenv().getOrDefault("TOPIC",
                        System.getenv().getOrDefault("TOPIC_NAME", "events"));
        int lingerMs   = Integer.parseInt(
                        System.getenv().getOrDefault("LINGER",
                        System.getenv().getOrDefault("LINGER_MS", "0")));
        int batchSize  = Integer.parseInt(
                        System.getenv().getOrDefault("BATCH",
                        System.getenv().getOrDefault("BATCH_SIZE", "16384")));
        long numMsg      = Long.parseLong(System.getenv().getOrDefault("NUM_MSG", "0")); // 0=無制限（時間で終了）
        int  payloadBytes = Integer.parseInt(System.getenv().getOrDefault("PAYLOAD_BYTES", "200"));
        long warmupMsgs   = Long.parseLong(System.getenv().getOrDefault("WARMUP_MSGS", "0"));
        String producerId = System.getenv().getOrDefault("HOSTNAME", UUID.randomUUID().toString());
        String keyStrategy   = System.getenv().getOrDefault("KEY_STRATEGY", "none"); // none|symbol|order_id
        int    HOT_KEY_EVERY = Integer.parseInt(System.getenv().getOrDefault("HOT_KEY_EVERY", "0"));
        String runId         = System.getenv().getOrDefault("RUN_ID", producerId);

        String[] symbols = {"AAPL","MSFT","AMZN","NVDA","GOOGL","META","TSLA","ORCL","AVGO","AMD"};
        java.util.Random rnd = new java.util.Random(7);

        char[] padChars = new char[payloadBytes];
        java.util.Arrays.fill(padChars, 'x');
        final String pad = new String(padChars);

        java.util.concurrent.atomic.AtomicLong sendErrors = new java.util.concurrent.atomic.AtomicLong();


        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idem);
        if (!idem) {
            // acks=1 の時は in-flight を少し増やしてスループット確保（任意）
            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        }
        props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-" + UUID.randomUUID());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");   // or lz4

        try (KafkaProducer<String,String> producer = new KafkaProducer<>(props)) {
            long i = 0;
            long runNs = Duration.ofSeconds(30).toNanos();
            long endNs = System.nanoTime() + runNs;

            while ((numMsg == 0 && System.nanoTime() < endNs) || (numMsg > 0 && i < numMsg)) {
                String symbol  = symbols[rnd.nextInt(symbols.length)];
                String orderId = "o-" + UUID.randomUUID();

                String key = switch (keyStrategy) {
                    case "symbol"   -> symbol;
                    case "order_id" -> orderId;
                    default -> {
                        if (HOT_KEY_EVERY > 0 && (i % HOT_KEY_EVERY) == 0) yield "HOT";
                        else yield "k-" + UUID.randomUUID();
                    }
                };

                // --- 送信時刻は単調クロックで（Consumer側と合わせる） ---
                long tsSendNs = System.currentTimeMillis() * 1_000_000L;

                // --- JSONペイロード（OrderEvent相当フィールド） ---
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

                producer.send(new ProducerRecord<>(topic, key, payload), (md, ex) -> {
                    if (ex != null) sendErrors.incrementAndGet();
                });
                i++;
            }
            producer.flush();
            System.out.println("送信完了: 件数=" + i + " send_errors=" + sendErrors.get());
        }
    }
}
