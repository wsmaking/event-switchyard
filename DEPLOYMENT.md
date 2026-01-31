# HFT Fast Path デプロイメントガイド

## 目次

1. [前提条件](#前提条件)
2. [ローカル開発環境](#ローカル開発環境)
3. [Docker環境](#docker環境)
4. [Kubernetes環境](#kubernetes環境)
5. [CI/CDパイプライン](#cicdパイプライン)
6. [トラブルシューティング](#トラブルシューティング)
7. [ロールバック手順](#ロールバック手順)

---

## 前提条件

### 必須環境

- **Java**: JDK 21以上 (Eclipse Temurin推奨)
- **Gradle**: 8.12.0以上 (Gradle Wrapper使用推奨)
- **Docker**: 20.10.0以上 (Kubernetes環境の場合)
- **Kubernetes**: 1.28以上 (本番環境の場合)
- **etcd**: 3.5.0以上 (分散コーディネーション)
- **Kafka**: 3.5.0以上 (イベントストリーミング)

### 推奨リソース

#### ローカル開発環境
- **CPU**: 4コア以上
- **メモリ**: 8GB以上
- **ストレージ**: 20GB以上の空き容量

#### 本番環境 (1ポッドあたり)
- **CPU**: 2コア (request: 1コア, limit: 2コア)
- **メモリ**: 2GB (request/limit固定)
- **ストレージ**: 10GB SSD (Chronicle Queue用)

---

## ローカル開発環境

### 1. クイックスタート

```bash
# 1. リポジトリクローン
git clone <repository-url>
cd event-switchyard

# 2. ビルド
./gradlew build

# 3. Fast Path有効化で起動
FAST_PATH_ENABLE=1 FAST_PATH_METRICS=1 ./gradlew run
```

### 2. 環境変数

| 変数名 | 必須 | デフォルト | 説明 |
|-------|------|----------|------|
| `FAST_PATH_ENABLE` | No | 0 | Fast Path機能を有効化 (1: 有効) |
| `FAST_PATH_METRICS` | No | 0 | Prometheusメトリクス公開 (1: 有効) |
| `KAFKA_BRIDGE_ENABLE` | No | 0 | Kafka連携を有効化 (1: 有効) |
| `ENGINE_ID` | No | e1 | エンジンインスタンスID |
| `ETCD_ENDPOINTS` | No | http://127.0.0.1:2379 | etcdエンドポイント (カンマ区切り) |
| `OWNED_KEYS` | No | - | 担当キー (カンマ区切り、例: BTC,ETH) |
| `ENGINE_POLL_MS` | No | 300 | etcd polling間隔 (ms) |

### 3. 動作確認

```bash
# ヘルスチェック
curl http://localhost:8080/health

# イベント送信
curl "http://localhost:8080/events?key=BTC" \
  -X POST -H "Content-Type: application/json" \
  -d '{"price": 50000}'

# 統計取得
curl http://localhost:8080/stats

# Prometheusメトリクス
curl http://localhost:8080/metrics
```

---

## Docker環境

### 1. Dockerイメージビルド

```bash
# イメージビルド
docker build -t hft-fast-path:latest .

# ビルド確認
docker images | grep hft-fast-path
```

### 2. Docker Composeでフルスタック起動

```bash
# 監視スタック込みで起動 (app + Prometheus + Grafana + Kafka)
docker-compose -f docker-compose.monitoring.yml up -d

# ログ確認
docker-compose -f docker-compose.monitoring.yml logs -f hft-app

# 停止
docker-compose -f docker-compose.monitoring.yml down
```

### 2.1 BackOffice Postgres マイグレーション

```bash
# Postgres起動（内部ネットワークのみ）
POSTGRES_PORT=5433 docker compose --profile gateway up -d postgres

# マイグレーション実行
POSTGRES_PORT=5433 docker compose --profile gateway run --rm backoffice-migrate
```

注意:
- `V1__backoffice_schema.sql` をjOOQ生成の都合で `VARCHAR` 型に寄せているため、既にV1を適用済みのDBではFlywayのチェックサム不一致が起きる。
- ローカル検証ではDBを作り直すのが簡単（例: `docker compose down -v` でボリューム削除）。
- 既存環境で進める場合は `flyway repair` を使う（運用での適用は別途手順整備が必要）。

### 2.2 Rust Gateway + BackOffice + Kafka (VM向け)

```bash
# Rust Gateway向けのフルスタック起動
docker compose -f docker-compose.rust.yml up -d

# BackOfficeのDBマイグレーション（初回のみ）
docker compose -f docker-compose.rust.yml run --rm backoffice-migrate
```

アクセス先:
- Gateway Rust: http://localhost:8081/health
- BackOffice: http://localhost:8082/health

### 3. アクセスURL

- **アプリケーション**: http://localhost:8080
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin)

### 4. Grafanaダッシュボード

1. ブラウザで http://localhost:3000 にアクセス
2. ログイン: `admin` / `admin`
3. 左メニュー > Dashboards > "HFT Fast Path"
4. 8つのパネルでメトリクスを可視化:
   - Fast Path Processing Latency (p50/p99/p999)
   - Fast Path Throughput
   - Fast Path p99 Gauge
   - Drop Rate
   - Persistence Queue Metrics
   - JVM Heap & Threads

---

## Kubernetes環境

### 1. 事前準備

#### a. Namespace作成

```bash
kubectl apply -f k8s/namespace.yaml
```

#### b. PriorityClass作成

```bash
kubectl apply -f k8s/priorityclass.yaml
```

#### c. ConfigMap作成

```bash
# ConfigMapを編集してetcd/Kafkaエンドポイントを設定
vim k8s/configmap.yaml

# 適用
kubectl apply -f k8s/configmap.yaml
```

**設定例**:
```yaml
data:
  etcd.endpoints: "http://etcd-0.etcd.svc.cluster.local:2379,http://etcd-1.etcd.svc.cluster.local:2379"
  kafka.bootstrap.servers: "kafka-0.kafka.svc.cluster.local:9092,kafka-1.kafka.svc.cluster.local:9092"
```

#### d. StorageClass作成 (未作成の場合)

```bash
# AWS EBS gp3の例
cat <<EOF | kubectl apply -f -
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: fast-ssd
provisioner: ebs.csi.aws.com
parameters:
  type: gp3
  iops: "3000"
  throughput: "125"
allowVolumeExpansion: true
volumeBindingMode: WaitForFirstConsumer
EOF
```

#### e. HFT専用ノードのラベリング

```bash
# HFT専用ノードにラベル付与
kubectl label nodes <node-name> node-role.kubernetes.io/hft=true
```

### 2. デプロイ

```bash
# PVC作成
kubectl apply -f k8s/pvc.yaml

# Service作成
kubectl apply -f k8s/service.yaml

# Deployment作成 (3レプリカ)
kubectl apply -f k8s/deployment.yaml
```

### 3. デプロイ確認

```bash
# Pod状態確認
kubectl get pods -n hft-system -l app=hft-fast-path

# ログ確認
kubectl logs -n hft-system -l app=hft-fast-path --tail=100

# ヘルスチェック (Pod内から)
kubectl exec -n hft-system <pod-name> -- curl -f http://localhost:8080/health

# メトリクス確認
kubectl exec -n hft-system <pod-name> -- curl http://localhost:8080/metrics
```

### 4. スケーリング

```bash
# レプリカ数変更 (例: 5レプリカ)
kubectl scale deployment/hft-fast-path -n hft-system --replicas=5

# HPA (Horizontal Pod Autoscaler) 設定
kubectl autoscale deployment hft-fast-path -n hft-system \
  --cpu-percent=70 --min=3 --max=10
```

### 5. ローリングアップデート

```bash
# イメージ更新
kubectl set image deployment/hft-fast-path \
  hft-fast-path=ghcr.io/<org>/hft-fast-path:v1.2.3 \
  -n hft-system

# ロールアウト状態確認
kubectl rollout status deployment/hft-fast-path -n hft-system

# ロールアウト履歴
kubectl rollout history deployment/hft-fast-path -n hft-system
```

---

## CI/CDパイプライン

### 1. GitHub Actions設定

#### a. リポジトリシークレット設定

GitHub Settings > Secrets and variables > Actions > New repository secret:

| シークレット名 | 説明 | 取得方法 |
|--------------|------|---------|
| `KUBE_CONFIG_STAGING` | ステージング環境のkubeconfig (base64) | `cat ~/.kube/config \| base64` |
| `KUBE_CONFIG_PRODUCTION` | 本番環境のkubeconfig (base64) | `cat ~/.kube/config-prod \| base64` |

#### b. パイプライン構成

[.github/workflows/deploy.yml](.github/workflows/deploy.yml):

1. **build-and-test**: ビルド & テスト実行
2. **docker-build-push**: Dockerイメージビルド & GitHub Container Registryにプッシュ
3. **deploy-staging**: ステージング環境デプロイ (`refactor_consumer_app`ブランチ)
4. **deploy-production**: 本番環境デプロイ (`main`ブランチ)

### 2. デプロイフロー

#### ステージング環境

```bash
# 1. featureブランチからrefactor_consumer_appへマージ
git checkout refactor_consumer_app
git merge feature/new-feature
git push origin refactor_consumer_app

# 2. GitHub ActionsでCI/CD自動実行
# - ビルド & テスト
# - Dockerイメージビルド
# - ステージング環境へ自動デプロイ

# 3. デプロイ確認
kubectl get pods -n hft-system --context=staging
```

#### 本番環境

```bash
# 1. refactor_consumer_appからmainへマージ (PRレビュー必須)
# GitHub上でPull Request作成 & レビュー & マージ

# 2. GitHub ActionsでCI/CD自動実行
# - ビルド & テスト
# - Dockerイメージビルド
# - 本番環境へ自動デプロイ
# - スモークテスト実行

# 3. デプロイ確認
kubectl get pods -n hft-system --context=production
```

### 3. 手動デプロイ (緊急時)

```bash
# 1. イメージビルド & プッシュ
docker build -t ghcr.io/<org>/hft-fast-path:hotfix-v1.2.4 .
docker push ghcr.io/<org>/hft-fast-path:hotfix-v1.2.4

# 2. Kubernetes更新
kubectl set image deployment/hft-fast-path \
  hft-fast-path=ghcr.io/<org>/hft-fast-path:hotfix-v1.2.4 \
  -n hft-system

# 3. ロールアウト確認
kubectl rollout status deployment/hft-fast-path -n hft-system
```

---

## トラブルシューティング

### 1. Pod起動失敗

#### 症状
```bash
kubectl get pods -n hft-system
# NAME                              READY   STATUS             RESTARTS   AGE
# hft-fast-path-xxx-yyy             0/1     CrashLoopBackOff   5          3m
```

#### 原因調査
```bash
# ログ確認
kubectl logs -n hft-system hft-fast-path-xxx-yyy --previous

# イベント確認
kubectl describe pod -n hft-system hft-fast-path-xxx-yyy
```

#### よくある原因

**A. etcd接続失敗**
```
Error: Connection refused: http://etcd:2379
```
対策:
```bash
# etcd Podが起動しているか確認
kubectl get pods -n etcd-system

# ConfigMapのetcd.endpointsを確認
kubectl get configmap hft-config -n hft-system -o yaml
```

**B. Kafka接続失敗**
```
Error: Failed to resolve 'kafka-0.kafka:9092'
```
対策:
```bash
# Kafka Podが起動しているか確認
kubectl get pods -n kafka

# ConfigMapのkafka.bootstrap.serversを確認
kubectl get configmap hft-config -n hft-system -o yaml
```

**C. JVMメモリ不足**
```
OutOfMemoryError: Java heap space
```
対策:
```bash
# Deployment内のJAVA_OPTSを調整
kubectl edit deployment hft-fast-path -n hft-system

# resources.limits.memoryを増やす
resources:
  limits:
    memory: "4Gi"  # 2Gi -> 4Gi
```

### 2. レイテンシ悪化

#### 症状
```bash
# p99レイテンシが100μsを超過
curl http://<pod-ip>:8080/stats
# "fast_path_process_p99_us": 250.5
```

#### 原因調査
```bash
# Prometheusメトリクス確認
kubectl port-forward -n hft-system svc/hft-fast-path 8080:8080
curl http://localhost:8080/metrics | grep fast_path

# JVMメトリクス確認
curl http://localhost:8080/metrics | grep jvm_
```

#### よくある原因

**A. GC頻発**
```
jvm_gc_pause_seconds_sum が増加傾向
```
対策:
```bash
# Deploymentの環境変数でJAVA_OPTSを調整
JAVA_OPTS="-Xms4g -Xmx4g -XX:+UseZGC -XX:MaxGCPauseMillis=1"
```

**B. Chronicle Queueディスク遅延**
```
persistence_queue_write_latency_p99_microseconds > 1000
```
対策:
```bash
# ストレージクラスを高速SSDに変更
kubectl get pvc -n hft-system
# StorageClassをfast-ssd (gp3, Premium SSD等) に変更
```

**C. CPU throttling**
```bash
# CPU使用率が制限値に達している
kubectl top pods -n hft-system
```
対策:
```bash
# CPU limitを増やす
kubectl edit deployment hft-fast-path -n hft-system
# resources.limits.cpu: 2000m -> 4000m
```

### 3. イベントドロップ発生

#### 症状
```bash
curl http://localhost:8080/stats
# "fast_path_drops_total": 152
```

#### 原因
- Disruptorリングバッファが満杯 (処理速度 < 受信速度)

#### 対策

**A. 短期対策: レプリカ数増加**
```bash
kubectl scale deployment/hft-fast-path -n hft-system --replicas=10
```

**B. 中期対策: リソース増強**
```bash
# CPU/メモリを増やす
kubectl edit deployment hft-fast-path -n hft-system
```

**C. 長期対策: アーキテクチャ見直し**
- Disruptorバッファサイズ拡大
- シャーディング導入 (キー範囲によるパーティショニング)

---

## ロールバック手順

### 1. Kubernetes環境

```bash
# 直前のリビジョンにロールバック
kubectl rollout undo deployment/hft-fast-path -n hft-system

# 特定のリビジョンにロールバック
kubectl rollout history deployment/hft-fast-path -n hft-system
kubectl rollout undo deployment/hft-fast-path -n hft-system --to-revision=3

# ロールバック状態確認
kubectl rollout status deployment/hft-fast-path -n hft-system
```

### 2. Docker Compose環境

```bash
# 1. 古いイメージタグに変更
vim docker-compose.monitoring.yml
# image: hft-fast-path:v1.2.2  # v1.2.3 -> v1.2.2

# 2. 再起動
docker-compose -f docker-compose.monitoring.yml down
docker-compose -f docker-compose.monitoring.yml up -d
```

### 3. ローカル環境

```bash
# 1. Gitで以前のコミットに戻す
git log --oneline
git checkout <commit-hash>

# 2. 再ビルド & 起動
./gradlew clean build
FAST_PATH_ENABLE=1 ./gradlew run
```

---

## 監視とアラート

### 1. Prometheusアラート設定 (推奨)

`prometheus-alerts.yml`:
```yaml
groups:
  - name: hft_critical
    rules:
      - alert: FastPathLatencyHigh
        expr: fast_path_process_latency_p99_microseconds > 100
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Fast Path p99レイテンシが目標超過 ({{ $value }}μs)"

      - alert: FastPathDropping
        expr: rate(fast_path_drops_total[1m]) > 0
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "Fast Pathでイベントドロップ発生"
```

### 2. ログ収集 (推奨)

- **Fluentd/Fluent Bit**: KubernetesログをElasticsearch/CloudWatch等に転送
- **Grafana Loki**: Prometheusと連携したログ集約

---

## 本番環境チェックリスト

### デプロイ前

- [ ] etcd/Kafkaクラスタが正常稼働
- [ ] HFT専用ノードのラベリング完了
- [ ] StorageClass (fast-ssd) 作成済み
- [ ] ConfigMapにエンドポイント設定済み
- [ ] Prometheusスクレイプ設定完了
- [ ] Grafanaダッシュボード確認
- [ ] アラート設定完了

### デプロイ後

- [ ] 全PodがReady状態
- [ ] ヘルスチェック成功 (`/health`)
- [ ] メトリクス公開確認 (`/metrics`)
- [ ] p99レイテンシ < 100μs
- [ ] ドロップ数 = 0
- [ ] GC停止時間 < 5ms
- [ ] Chronicle Queue書き込み正常

---

## 参考資料

- [README_HFT.md](README_HFT.md) - HFT Fast Path機能ガイド
- [PHASE5_PROMETHEUS_GRAFANA.md](PHASE5_PROMETHEUS_GRAFANA.md) - 監視基盤構築ガイド
- [PROJECT_STATUS.md](PROJECT_STATUS.md) - プロジェクト全体の進捗
- [Dockerfile](Dockerfile) - コンテナビルド設定
- [k8s/](k8s/) - Kubernetesマニフェスト
- [.github/workflows/deploy.yml](.github/workflows/deploy.yml) - CI/CDパイプライン

---

**ドキュメント作成日**: 2025-11-02
**対象バージョン**: v1.0.0
**ステータス**: 本番環境対応済み
