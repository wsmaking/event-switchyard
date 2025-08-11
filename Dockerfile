
# ===== Build stage (Gradle入り) =====
FROM gradle:8.12.0-jdk21 AS builder
WORKDIR /workspace

# 依存キャッシュを効かせるためにメタだけ先にコピー
COPY settings.gradle ./
COPY app/build.gradle app/build.gradle
# もし version catalog 等を使っているならコメント解除
# COPY gradle gradle
# COPY gradle.properties gradle.properties

# 依存だけ先に解決（キャッシュ利用）
RUN --mount=type=cache,target=/home/gradle/.gradle \
    gradle --no-daemon :app:dependencies > /dev/null || true

# 残りのソースをコピーしてビルド（fat-jar）
COPY . .
RUN gradle --no-daemon :app:shadowJar

# ===== Runtime stage (JREのみ) =====
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=builder /workspace/app/build/libs/app-all.jar /app/app-all.jar
# 実行は docker-compose 側の command で指定（Producer/Consumer/TopicInit など）
