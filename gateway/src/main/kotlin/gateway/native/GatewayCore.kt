package gateway.native

import com.sun.jna.Library
import com.sun.jna.Native
import com.sun.jna.Pointer

/**
 * Rust実装のgateway-coreへのJNAブリッジ
 *
 * ## JNA vs JNI
 * JNAを使用することで、C関数名をそのまま呼び出せる。
 * JNIだと関数名を Java_パッケージ_クラス_メソッド 形式に合わせる必要があるが、
 * JNAならRust側の #[no_mangle] extern "C" 関数をそのまま使える。
 *
 * ## 使用方法
 * ```kotlin
 * GatewayCore.init(65536)  // 初期化（キュー容量）
 * val result = GatewayCore.processOrder(orderId, accountId, symbol, side, qty, price, timestampNs)
 * ```
 */
object GatewayCore {

    /** 処理結果コード（Rust側のresult_codesと対応） */
    object ResultCode {
        const val ACCEPTED = 0
        const val REJECTED_MAX_QTY = 1
        const val REJECTED_MAX_NOTIONAL = 2
        const val REJECTED_DAILY_LIMIT = 3
        const val REJECTED_UNKNOWN_SYMBOL = 4
        const val ERROR_NOT_INITIALIZED = -1
        const val ERROR_QUEUE_FULL = -2
        const val ERROR_INVALID_SYMBOL = -3
    }

    /**
     * JNA インターフェース定義
     * Rust側の extern "C" 関数と1:1対応
     */
    private interface GatewayCoreLib : Library {
        fun gateway_core_init(queueCapacity: Int): Int
        fun gateway_core_process_order(
            orderId: Long,
            accountId: Long,
            symbolPtr: Pointer,
            side: Byte,
            qty: Int,
            price: Long,
            timestampNs: Long
        ): Int
        fun gateway_core_pop_order(
            outOrderId: LongArray,
            outAccountId: LongArray,
            outSymbol: ByteArray,
            outSide: ByteArray,
            outQty: IntArray,
            outPrice: LongArray,
            outTimestampNs: LongArray
        ): Int
        fun gateway_core_queue_len(): Int
        fun gateway_core_queue_capacity(): Int
        fun gateway_core_register_symbol(symbolPtr: Pointer, maxOrderQty: Int, maxNotional: Long): Int
        fun gateway_core_latency_p50(): Long
        fun gateway_core_latency_p99(): Long
        fun gateway_core_latency_max(): Long
        fun gateway_core_latency_count(): Long
        fun gateway_core_latency_reset()
    }

    private val lib: GatewayCoreLib by lazy {
        val libPath = System.getenv("GATEWAY_CORE_LIB_PATH")
        if (libPath != null) {
            Native.load(libPath, GatewayCoreLib::class.java)
        } else {
            Native.load("gateway_core", GatewayCoreLib::class.java)
        }
    }

    @Volatile
    private var initialized = false

    /**
     * Rustコア初期化
     * @param queueCapacity キュー容量（2のべき乗に丸められる）
     * @return 0=成功, -1=既に初期化済み
     */
    @Synchronized
    fun init(queueCapacity: Int = 65536): Int {
        if (initialized) return -1
        val result = lib.gateway_core_init(queueCapacity)
        if (result == 0) initialized = true
        return result
    }

    /**
     * 注文処理（リスクチェック + キュー投入）
     *
     * @param orderId 注文ID
     * @param accountId 口座ID
     * @param symbol 銘柄コード（8バイト、不足分は0埋め）
     * @param side 売買方向（1=買い, 2=売り）
     * @param qty 数量
     * @param price 価格
     * @param timestampNs タイムスタンプ（ナノ秒）
     * @return ResultCodeの値
     */
    fun processOrder(
        orderId: Long,
        accountId: Long,
        symbol: ByteArray,
        side: Byte,
        qty: Int,
        price: Long,
        timestampNs: Long
    ): Int {
        require(symbol.size == 8) { "symbol must be exactly 8 bytes" }
        val mem = com.sun.jna.Memory(8)
        mem.write(0, symbol, 0, 8)
        return lib.gateway_core_process_order(orderId, accountId, mem, side, qty, price, timestampNs)
    }

    /**
     * 文字列シンボル版
     */
    fun processOrder(
        orderId: Long,
        accountId: Long,
        symbol: String,
        side: Byte,
        qty: Int,
        price: Long,
        timestampNs: Long
    ): Int = processOrder(orderId, accountId, symbolToBytes(symbol), side, qty, price, timestampNs)

    /**
     * キューから注文を取り出す
     * @return 取り出した注文、またはnull（キューが空の場合）
     */
    fun popOrder(): PopResult? {
        val orderId = LongArray(1)
        val accountId = LongArray(1)
        val symbol = ByteArray(8)
        val side = ByteArray(1)
        val qty = IntArray(1)
        val price = LongArray(1)
        val timestampNs = LongArray(1)

        val result = lib.gateway_core_pop_order(orderId, accountId, symbol, side, qty, price, timestampNs)
        return if (result == 1) {
            PopResult(orderId[0], accountId[0], symbol, side[0], qty[0], price[0], timestampNs[0])
        } else {
            null
        }
    }

    data class PopResult(
        val orderId: Long,
        val accountId: Long,
        val symbol: ByteArray,
        val side: Byte,
        val qty: Int,
        val price: Long,
        val timestampNs: Long
    ) {
        fun symbolString(): String = bytesToSymbol(symbol)
    }

    /** キュー長を取得 */
    fun queueLen(): Int = lib.gateway_core_queue_len()

    /** キュー容量を取得 */
    fun queueCapacity(): Int = lib.gateway_core_queue_capacity()

    /**
     * 銘柄固有のリスク上限を登録
     */
    fun registerSymbol(symbol: String, maxOrderQty: Int, maxNotional: Long): Int {
        val bytes = symbolToBytes(symbol)
        val mem = com.sun.jna.Memory(8)
        mem.write(0, bytes, 0, 8)
        return lib.gateway_core_register_symbol(mem, maxOrderQty, maxNotional)
    }

    /** レイテンシp50（ナノ秒） */
    fun latencyP50(): Long = lib.gateway_core_latency_p50()

    /** レイテンシp99（ナノ秒） */
    fun latencyP99(): Long = lib.gateway_core_latency_p99()

    /** レイテンシ最大値（ナノ秒） */
    fun latencyMax(): Long = lib.gateway_core_latency_max()

    /** レイテンシサンプル数 */
    fun latencyCount(): Long = lib.gateway_core_latency_count()

    /** レイテンシ統計をリセット */
    fun latencyReset() = lib.gateway_core_latency_reset()

    // =====================================================
    // ヘルパーメソッド
    // =====================================================

    /**
     * 銘柄文字列を8バイト配列に変換
     */
    fun symbolToBytes(symbol: String): ByteArray {
        val bytes = ByteArray(8)
        val src = symbol.toByteArray(Charsets.US_ASCII)
        System.arraycopy(src, 0, bytes, 0, minOf(src.size, 8))
        return bytes
    }

    /**
     * 8バイト配列を銘柄文字列に変換
     */
    fun bytesToSymbol(bytes: ByteArray): String {
        val end = bytes.indexOf(0).let { if (it < 0) bytes.size else it }
        return String(bytes, 0, end, Charsets.US_ASCII)
    }

    /**
     * 結果コードを人間可読な文字列に変換
     */
    fun resultCodeToString(code: Int): String = when (code) {
        ResultCode.ACCEPTED -> "ACCEPTED"
        ResultCode.REJECTED_MAX_QTY -> "REJECTED_MAX_QTY"
        ResultCode.REJECTED_MAX_NOTIONAL -> "REJECTED_MAX_NOTIONAL"
        ResultCode.REJECTED_DAILY_LIMIT -> "REJECTED_DAILY_LIMIT"
        ResultCode.REJECTED_UNKNOWN_SYMBOL -> "REJECTED_UNKNOWN_SYMBOL"
        ResultCode.ERROR_NOT_INITIALIZED -> "ERROR_NOT_INITIALIZED"
        ResultCode.ERROR_QUEUE_FULL -> "ERROR_QUEUE_FULL"
        ResultCode.ERROR_INVALID_SYMBOL -> "ERROR_INVALID_SYMBOL"
        else -> "UNKNOWN($code)"
    }
}
