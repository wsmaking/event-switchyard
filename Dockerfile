# ===== Build stage (Gradle入り) =====
FROM gradle:8.12.0-jdk21 AS builder
WORKDIR /workspace

# 依存キャッシュ用メタ（root側があれば含める）
COPY settings.gradle* ./
COPY build.gradle* ./

# ★ ここで version catalog 等を入れる（今回の肝）
COPY gradle gradle
COPY gradle.properties gradle.properties

# app 側の build スクリプト
COPY app/build.gradle app/build.gradle

# 依存だけ先に解決（キャッシュ利用）
RUN --mount=type=cache,target=/home/gradle/.gradle \
    gradle --no-daemon :app:dependencies || true

# ★ ソースをコピー（Engine.kt含む）
COPY app/src /workspace/app/src

# ★ 中身ガード（polling版が入っているか確認）
RUN set -eux; \
    test -f app/src/main/kotlin/engine/Engine.kt; \
    grep -n "polling mode" app/src/main/kotlin/engine/Engine.kt; \
    ! grep -R "watch(" app/src/main/kotlin || true

# 失敗行の周辺を番号付きで出す（80行まで必要なら数字を伸ばす）
RUN set -eux; sed -n '1,120p' app/src/main/kotlin/engine/Engine.kt | nl -ba

#  verbose ビルド
RUN --mount=type=cache,target=/home/gradle/.gradle \
    gradle --no-daemon --info --stacktrace :app:shadowJar

# fat-jar を作る
RUN --mount=type=cache,target=/home/gradle/.gradle \
    gradle --no-daemon :app:shadowJar

# ===== Runtime stage (JREのみ) =====
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=builder /workspace/app/build/libs/app-all.jar /app/app-all.jar
