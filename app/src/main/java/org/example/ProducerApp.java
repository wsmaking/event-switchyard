// ProducerApp.java（主要部）
package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerApp {

    public static void main(String[] args) throws Exception {
        String brokers   = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "kafka:9092");
        String topic     = System.getenv().getOrDefault("TOPIC_NAME", "events");
        String acks      = System.getenv().getOrDefault("ACKS", "all");
        int lingerMs     = Integer.parseInt(System.getenv().getOrDefault("LINGER_MS", "0"));
        int batchSize    = Integer.parseInt(System.getenv().getOrDefault("BATCH_SIZE", "16384"));
        long numMsg      = Long.parseLong(System.getenv().getOrDefault("NUM_MSG", "0")); // 0=無制限（時間で終了）
        int  payloadBytes = Integer.parseInt(System.getenv().getOrDefault("PAYLOAD_BYTES", "200"));
        long warmupMsgs   = Long.parseLong(System.getenv().getOrDefault("WARMUP_MSGS", "0"));
        String producerId = System.getenv().getOrDefault("HOSTNAME", UUID.randomUUID().toString());

        char[] padChars = new char[payloadBytes];
        java.util.Arrays.fill(padChars, 'x');
        final String pad = new String(padChars);

        java.util.concurrent.atomic.AtomicLong sendErrors = new java.util.concurrent.atomic.AtomicLong();

        int  HOT_KEY_EVERY   = Integer.parseInt(System.getenv().getOrDefault("HOT_KEY_EVERY", "0"));


        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-" + UUID.randomUUID());

        ObjectMapper mapper = new ObjectMapper();

        try (KafkaProducer<String,String> producer = new KafkaProducer<>(props)) {
            long i = 0;
            long runMs = Duration.ofSeconds(30).toMillis(); // ベンチは30秒を既定
            long end   = System.currentTimeMillis() + runMs;

            while ((numMsg == 0 && System.currentTimeMillis() < end) || (numMsg > 0 && i < numMsg)) {
                // キー生成
                String key;
                if (HOT_KEY_EVERY > 0 && (i % HOT_KEY_EVERY) == 0) {
                    key = "HOT"; // ← 偏らせる
                } else {
                    key = "k-" + UUID.randomUUID(); // ← 均等化（hash分散）
                }

                long tsNs = System.currentTimeMillis() * 1_000_000L; // 壁時計→ns
                var node = mapper.createObjectNode()
                    .put("seq", i)
                    .put("ts_ns", tsNs)
                    .put("producer_id", producerId)
                    .put("is_warmup", i < warmupMsgs)
                    .put("pad", pad); // サイズ調整用

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
