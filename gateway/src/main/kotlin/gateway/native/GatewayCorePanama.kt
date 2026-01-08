package gateway.native

import java.lang.foreign.*
import java.lang.invoke.MethodHandle
import java.nio.file.Path

/**
 * Panama (Foreign Function & Memory API) を使用した Rust FFI ブリッジ
 *
 * ## JNA との違い
 * - JNA: 実行時にリフレクションで関数を解決 → オーバーヘッド ~3μs
 * - Panama: 起動時に MethodHandle を取得、JVM が最適化可能 → オーバーヘッド ~50-100ns
 *
 * ## 使用方法
 * ```kotlin
 * GatewayCorePanama.init(65536)
 * val result = GatewayCorePanama.processOrder(orderId, accountId, "AAPL", 1, 100, 15000, timestamp)
 * ```
 *
 * ## 注意
 * - Java 21+ 必須（Foreign Function & Memory API が正式リリース）
 * - JVM 起動時に --enable-native-access=ALL-UNNAMED が必要
 */
object GatewayCorePanama {

    // ===========================================
    // 定数: リスクチェック結果コード
    // ===========================================
    const val ACCEPTED = 0
    const val REJECTED_MAX_QTY = 1
    const val REJECTED_MAX_NOTIONAL = 2
    const val REJECTED_DAILY_LIMIT = 3
    const val REJECTED_UNKNOWN_SYMBOL = 4
    const val ERROR_NOT_INITIALIZED = -1
    const val ERROR_QUEUE_FULL = -2
    const val ERROR_INVALID_SYMBOL = -3

    // ===========================================
    // ネイティブリンカーとライブラリ
    // ===========================================
    private val linker: Linker = Linker.nativeLinker()
    private val arena: Arena = Arena.global()

    private val symbolLookup: SymbolLookup by lazy {
        // 環境変数またはリソースからライブラリパスを取得
        val libPath = System.getenv("GATEWAY_CORE_LIB_PATH")
            ?: throw IllegalStateException("GATEWAY_CORE_LIB_PATH environment variable not set")

        SymbolLookup.libraryLookup(Path.of(libPath), arena)
    }

    // ===========================================
    // 関数ハンドル（起動時に1回だけ解決）
    // ===========================================

    /** gateway_core_init(queue_capacity: u32) -> i32 */
    private val initHandle: MethodHandle by lazy {
        val desc = FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT)
        linker.downcallHandle(symbolLookup.find("gateway_core_init").orElseThrow(), desc)
    }

    /** gateway_core_process_order(...) -> i32 */
    private val processOrderHandle: MethodHandle by lazy {
        val desc = FunctionDescriptor.of(
            ValueLayout.JAVA_INT,      // 戻り値
            ValueLayout.JAVA_LONG,     // order_id
            ValueLayout.JAVA_LONG,     // account_id
            ValueLayout.ADDRESS,       // symbol_ptr
            ValueLayout.JAVA_BYTE,     // side
            ValueLayout.JAVA_INT,      // qty
            ValueLayout.JAVA_LONG,     // price
            ValueLayout.JAVA_LONG      // timestamp_ns
        )
        linker.downcallHandle(symbolLookup.find("gateway_core_process_order").orElseThrow(), desc)
    }

    /** gateway_core_pop_order(...) -> i32 */
    private val popOrderHandle: MethodHandle by lazy {
        val desc = FunctionDescriptor.of(
            ValueLayout.JAVA_INT,      // 戻り値
            ValueLayout.ADDRESS,       // out_order_id
            ValueLayout.ADDRESS,       // out_account_id
            ValueLayout.ADDRESS,       // out_symbol
            ValueLayout.ADDRESS,       // out_side
            ValueLayout.ADDRESS,       // out_qty
            ValueLayout.ADDRESS,       // out_price
            ValueLayout.ADDRESS        // out_timestamp_ns
        )
        linker.downcallHandle(symbolLookup.find("gateway_core_pop_order").orElseThrow(), desc)
    }

    /** gateway_core_queue_len() -> i32 */
    private val queueLenHandle: MethodHandle by lazy {
        val desc = FunctionDescriptor.of(ValueLayout.JAVA_INT)
        linker.downcallHandle(symbolLookup.find("gateway_core_queue_len").orElseThrow(), desc)
    }

    /** gateway_core_latency_p50() -> u64 */
    private val latencyP50Handle: MethodHandle by lazy {
        val desc = FunctionDescriptor.of(ValueLayout.JAVA_LONG)
        linker.downcallHandle(symbolLookup.find("gateway_core_latency_p50").orElseThrow(), desc)
    }

    /** gateway_core_latency_p99() -> u64 */
    private val latencyP99Handle: MethodHandle by lazy {
        val desc = FunctionDescriptor.of(ValueLayout.JAVA_LONG)
        linker.downcallHandle(symbolLookup.find("gateway_core_latency_p99").orElseThrow(), desc)
    }

    /** gateway_core_latency_max() -> u64 */
    private val latencyMaxHandle: MethodHandle by lazy {
        val desc = FunctionDescriptor.of(ValueLayout.JAVA_LONG)
        linker.downcallHandle(symbolLookup.find("gateway_core_latency_max").orElseThrow(), desc)
    }

    /** gateway_core_latency_reset() */
    private val latencyResetHandle: MethodHandle by lazy {
        val desc = FunctionDescriptor.ofVoid()
        linker.downcallHandle(symbolLookup.find("gateway_core_latency_reset").orElseThrow(), desc)
    }

    // ===========================================
    // 再利用可能なメモリセグメント（アロケーション削減）
    // ===========================================

    /** シンボル用の8バイトバッファ（スレッドローカルで競合回避） */
    private val symbolBuffer: ThreadLocal<MemorySegment> = ThreadLocal.withInitial {
        arena.allocate(8)
    }

    // ===========================================
    // 公開 API
    // ===========================================

    /**
     * ライブラリ初期化
     *
     * @param queueCapacity キュー容量（2のべき乗に丸められる）
     * @return 0: 成功, -1: 既に初期化済み
     */
    fun init(queueCapacity: Int): Int {
        return initHandle.invokeExact(queueCapacity) as Int
    }

    /**
     * 注文処理（リスクチェック + キュー投入）
     *
     * @param orderId 注文ID
     * @param accountId 口座ID
     * @param symbol 銘柄コード（8文字以下）
     * @param side 売買方向（1=買い, 2=売り）
     * @param qty 数量
     * @param price 価格
     * @param timestampNs タイムスタンプ（ナノ秒）
     * @return 結果コード（ACCEPTED, REJECTED_*, ERROR_*）
     */
    fun processOrder(
        orderId: Long,
        accountId: Long,
        symbol: String,
        side: Byte,
        qty: Int,
        price: Long,
        timestampNs: Long
    ): Int {
        // シンボルをネイティブメモリにコピー（8バイト固定）
        val symbolSeg = symbolBuffer.get()
        val bytes = symbol.toByteArray(Charsets.US_ASCII)
        for (i in 0 until 8) {
            symbolSeg.set(ValueLayout.JAVA_BYTE, i.toLong(), if (i < bytes.size) bytes[i] else 0)
        }

        return processOrderHandle.invokeExact(
            orderId,
            accountId,
            symbolSeg,
            side,
            qty,
            price,
            timestampNs
        ) as Int
    }

    /**
     * キューから注文を取り出す
     *
     * @return 注文データ、またはキューが空の場合は null
     */
    fun popOrder(): OrderData? {
        Arena.ofConfined().use { tempArena ->
            val outOrderId = tempArena.allocate(ValueLayout.JAVA_LONG)
            val outAccountId = tempArena.allocate(ValueLayout.JAVA_LONG)
            val outSymbol = tempArena.allocate(8)
            val outSide = tempArena.allocate(ValueLayout.JAVA_BYTE)
            val outQty = tempArena.allocate(ValueLayout.JAVA_INT)
            val outPrice = tempArena.allocate(ValueLayout.JAVA_LONG)
            val outTimestampNs = tempArena.allocate(ValueLayout.JAVA_LONG)

            val result = popOrderHandle.invokeExact(
                outOrderId,
                outAccountId,
                outSymbol,
                outSide,
                outQty,
                outPrice,
                outTimestampNs
            ) as Int

            return if (result == 1) {
                // シンボルをバイト配列として読み取り、文字列に変換
                val symbolBytes = ByteArray(8) { i -> outSymbol.get(ValueLayout.JAVA_BYTE, i.toLong()) }
                val symbolStr = String(symbolBytes, Charsets.US_ASCII).trimEnd('\u0000')

                OrderData(
                    orderId = outOrderId.get(ValueLayout.JAVA_LONG, 0),
                    accountId = outAccountId.get(ValueLayout.JAVA_LONG, 0),
                    symbol = symbolStr,
                    side = outSide.get(ValueLayout.JAVA_BYTE, 0),
                    qty = outQty.get(ValueLayout.JAVA_INT, 0),
                    price = outPrice.get(ValueLayout.JAVA_LONG, 0),
                    timestampNs = outTimestampNs.get(ValueLayout.JAVA_LONG, 0)
                )
            } else {
                null
            }
        }
    }

    /**
     * キュー長を取得
     */
    fun queueLen(): Int {
        return queueLenHandle.invokeExact() as Int
    }

    /**
     * レイテンシ統計: p50
     */
    fun latencyP50(): Long {
        return latencyP50Handle.invokeExact() as Long
    }

    /**
     * レイテンシ統計: p99
     */
    fun latencyP99(): Long {
        return latencyP99Handle.invokeExact() as Long
    }

    /**
     * レイテンシ統計: 最大値
     */
    fun latencyMax(): Long {
        return latencyMaxHandle.invokeExact() as Long
    }

    /**
     * レイテンシ統計をリセット
     */
    fun latencyReset() {
        latencyResetHandle.invokeExact()
    }

    /**
     * 注文データ
     */
    data class OrderData(
        val orderId: Long,
        val accountId: Long,
        val symbol: String,
        val side: Byte,
        val qty: Int,
        val price: Long,
        val timestampNs: Long
    )
}
