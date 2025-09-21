package org.example;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * トピック初期化ユーティリティ。
 *
 * 目的:
 * - ブローカ起動を待ち、指定された topic が無ければ作成。
 * - 既存ならパーティション数／レプリケーションの状態を点検し、
 *   パーティション不足時は増やす。
 * - 最後に "使える状態"（最低限メタデータを引ける）まで待ってから終了。
 */
public class TopicInit {
  public static void main(String[] args) throws Exception {
    // ---- 環境変数から設定を受け取る（デフォルトも用意）
    // KafkaブローカのブートストラップURL（複数ならカンマ区切り）。Docker Compose等では "kafka:9092" になりがち。
    final String bootstrap   = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "kafka:9092");

    // トピック名。TOPIC_NAME が無ければ TOPIC、それも無ければ "events"
    final String topic = Optional.ofNullable(System.getenv("TOPIC_NAME"))
            .orElse(System.getenv().getOrDefault("TOPIC", "events"));

    // パーティション数とレプリケーション数
    final int    partitions  = Integer.parseInt(System.getenv().getOrDefault("PARTITIONS", "1"));
    final short  replication = Short.parseShort(System.getenv().getOrDefault("REPLICATION", "1"));

    // 既存トピックでパーティションが不足している場合に限り「増やす」ことを許可するフラグ
    final boolean allowIncreasePartitions =
        !"0".equals(System.getenv().getOrDefault("ALLOW_INCREASE_PARTITIONS", "1"));

    // ---- AdminClient の基本プロパティ
    Properties p = new Properties();
    p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    // Admin リクエストのタイムアウト/リトライ。クラスタが立ち上がり中でもなるべく待つ。
    p.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
    p.put(AdminClientConfig.RETRIES_CONFIG, "100");

    try (AdminClient admin = AdminClient.create(p)) {
      // ---- ブローカ起動待ち（最大30秒）
      // 目的: docker-compose などでブローカの立ち上がりが遅い場合にリトライしつつ待機。
      long deadline = System.currentTimeMillis() + 30_000;
      while (true) {
        try {
          // nodes() ：ブローカ一覧を取得 Admin API。get() で完了まで待つ。
          admin.describeCluster().nodes().get();
          break; // 成功したら抜ける
        } catch (Exception e) {
          if (System.currentTimeMillis() > deadline) throw e; // 期限超過なら失敗として投げる
          Thread.sleep(500); // 少し待って再試行（ポーリング）
        }
      }

      // ---- トピックの既存チェック
      // listInternal(false): 内部トピック（__consumer_offsets など）を除外して一覧取得
      boolean exists = admin.listTopics(new ListTopicsOptions().listInternal(false))
                           .names().get()                // Future<Set<String>> を取得
                           .contains(topic);             // 目的の名前があるか

      if (!exists) {
        // ---- 新規作成フロー
        NewTopic newTopic = new NewTopic(topic, partitions, replication);
        try {
          // 全ブローカに伝搬するまで待機（all().get()）
          admin.createTopics(Collections.singleton(newTopic)).all().get();
          System.out.println("created topic: " + topic
              + " partitions=" + partitions + " rf=" + replication);
        } catch (ExecutionException ee) {
          // 競合（他プロセスでほぼ同時に作成された）場合は TopicExists として扱い、先へ進む
          if (ee.getCause() instanceof TopicExistsException) {
            System.out.println("topic exists (race): " + topic);
          } else {
            throw ee; // それ以外の失敗は上位へ
          }
        }
      } else {
        // ---- 既存トピックの状態を点検
        TopicDescription td = admin.describeTopics(Collections.singleton(topic))
                                   .values().get(topic).get();
        int currentPartitions = td.partitions().size();

        // 念のため最小値を取って「最小RF」を点検する。
        short minRf = Short.MAX_VALUE;
        for (TopicPartitionInfo pi : td.partitions()) {
          minRf = (short)Math.min(minRf, pi.replicas().size());
        }

        // ---- パーティション不足なら増やす（※減らすことは不可）
        // 既存データの並び順やキー分布に影響し得るため、増やす可否はフラグで制御。
        if (allowIncreasePartitions && partitions > currentPartitions) {
          admin.createPartitions(Collections.singletonMap(
              topic, NewPartitions.increaseTo(partitions)
          )).all().get();
          System.out.println("increased partitions: " + topic
              + " " + currentPartitions + " -> " + partitions);
        }

        // ---- RF 警告（差異がある場合）
        // AdminClient だけでは RF 変更は完結できない（割当の再計算と再割当が必要）ので警告のみ。
        if (replication != minRf) {
          System.out.println("WARN: replication factor differs. current(min)="
              + minRf + " desired=" + replication
              + " (change RF requires partition reassignment)");
        }
      }

      // ---- "使える状態" の最終確認
      // メタデータが取得できる＝名前解決できる・リーダ割当が済んでいる見込みのReady判定。
      admin.describeTopics(Collections.singleton(topic)).all().get();
      System.out.println("topic ready: " + topic);
    }
  }
}
