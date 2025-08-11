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
        long tsNs = System.currentTimeMillis() * 1_000_000L; // 壁時計→ns
        String payload = mapper.createObjectNode()
            .put("seq", i)
            .put("ts_ns", tsNs)
            .toString();

        producer.send(new ProducerRecord<>(topic, "key"+i, payload));
        i++;
      }
      producer.flush();
      System.out.println("送信完了: 件数 = " + i);
    }
  }
}
