# ===== Build stage (Gradle入り) =====
FROM gradle:8.12.0-jdk21 AS builder
WORKDIR /workspace

# Node.jsインストール（フロントエンドビルド用）
RUN apt-get update && apt-get install -y curl && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# 依存キャッシュ用メタ（root側があれば含める）
COPY settings.gradle* ./
COPY build.gradle* ./

# Version catalog & Gradle wrapper
COPY gradle gradle
COPY gradle.properties gradle.properties

# app 側の build スクリプト
COPY app/build.gradle app/build.gradle

# 依存だけ先に解決（キャッシュ利用）
RUN --mount=type=cache,target=/home/gradle/.gradle \
    gradle --no-daemon :app:dependencies || true

# フロントエンドをコピー（ビルドに必要）
COPY frontend /workspace/frontend

# ソースをコピー
COPY app/src /workspace/app/src

# fat-jar を作る（フロントエンドも自動ビルドされる）
RUN --mount=type=cache,target=/home/gradle/.gradle \
    gradle --no-daemon :app:shadowJar

# ===== Runtime stage (JRE + HFT最適化) =====
FROM eclipse-temurin:21-jre
WORKDIR /app

# Chronicle Queue用データディレクトリ作成
RUN mkdir -p /app/var/logs /app/var/chronicle-queue

# fat-jarをコピー
COPY --from=builder /workspace/app/build/libs/app-all.jar /app/app-all.jar

# HFT最適化JVMフラグ
# - ZGC: 低レイテンシGC
# - 固定ヒープ2GB (Xms=Xmx): GC頻度削減
# - MaxGCPauseMillis: GC停止時間目標1ms
# - AlwaysPreTouch: 起動時にヒープを物理メモリに確保
ENV JAVA_OPTS="-Xms2g -Xmx2g -XX:+UseZGC -XX:MaxGCPauseMillis=1 -XX:+AlwaysPreTouch -XX:+DisableExplicitGC"

# ポート公開
EXPOSE 8080

# ヘルスチェック
HEALTHCHECK --interval=30s --timeout=3s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# 実行
CMD ["sh", "-c", "java $JAVA_OPTS -jar /app/app-all.jar"]
