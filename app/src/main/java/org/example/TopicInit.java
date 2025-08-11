package org.example;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class TopicInit {
  public static void main(String[] args) throws Exception {
    final String bootstrap   = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "kafka:9092");
    final String topic = Optional.ofNullable(System.getenv("TOPIC_NAME"))
            .orElse(System.getenv().getOrDefault("TOPIC", "events"));
    final int    partitions  = Integer.parseInt(System.getenv().getOrDefault("PARTITIONS", "1"));
    final short  replication = Short.parseShort(System.getenv().getOrDefault("REPLICATION", "1"));
    final boolean allowIncreasePartitions =
        !"0".equals(System.getenv().getOrDefault("ALLOW_INCREASE_PARTITIONS", "1"));

    Properties p = new Properties();
    p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
    p.put(AdminClientConfig.RETRIES_CONFIG, "100");

    try (AdminClient admin = AdminClient.create(p)) {
      // ブローカ起動待ち（最大30秒）
      long deadline = System.currentTimeMillis() + 30_000;
      while (true) {
        try {
          admin.describeCluster().nodes().get();
          break;
        } catch (Exception e) {
          if (System.currentTimeMillis() > deadline) throw e;
          Thread.sleep(500);
        }
      }

      // 既存チェック
      boolean exists = admin.listTopics(new ListTopicsOptions().listInternal(false))
                           .names().get().contains(topic);

      if (!exists) {
        // 新規作成
        NewTopic newTopic = new NewTopic(topic, partitions, replication);
        try {
          admin.createTopics(Collections.singleton(newTopic)).all().get();
          System.out.println("created topic: " + topic
              + " partitions=" + partitions + " rf=" + replication);
        } catch (ExecutionException ee) {
          if (ee.getCause() instanceof TopicExistsException) {
            System.out.println("topic exists (race): " + topic);
          } else {
            throw ee;
          }
        }
      } else {
        // 既存の状態を点検
        TopicDescription td = admin.describeTopics(Collections.singleton(topic))
                                   .values().get(topic).get();
        int currentPartitions = td.partitions().size();
        // RF はパーティションごとに取りうるので最小値を見ておく
        short minRf = Short.MAX_VALUE;
        for (TopicPartitionInfo pi : td.partitions()) {
          minRf = (short)Math.min(minRf, pi.replicas().size());
        }

        // パーティション数が不足していたら増加（減少は不可）
        if (allowIncreasePartitions && partitions > currentPartitions) {
          admin.createPartitions(Collections.singletonMap(
              topic, NewPartitions.increaseTo(partitions)
          )).all().get();
          System.out.println("increased partitions: " + topic
              + " " + currentPartitions + " -> " + partitions);
        }

        // RF が希望値と違う場合は警告（再割当は別ツールで行う）
        if (replication != minRf) {
          System.out.println("WARN: replication factor differs. current(min)="
              + minRf + " desired=" + replication
              + " (change RF requires partition reassignment)");
        }
      }

      // 最終的に “使える状態” まで待つ（名前解決でき、少なくとも1リーダあり）
      admin.describeTopics(Collections.singleton(topic)).all().get();
      System.out.println("topic ready: " + topic);
    }
  }
}
