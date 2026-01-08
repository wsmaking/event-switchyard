package gateway.rate

/**
 * 簡易の秒間レート制限（全体で1本）。
 * 高頻度の突発負荷で入口を守るための最小実装。
 */
class RateLimiter(
    private val limitPerSecond: Long
) {
    @Volatile
    private var windowSec: Long = 0
    @Volatile
    private var count: Long = 0

    @Synchronized
    fun allow(nowMillis: Long = System.currentTimeMillis()): Boolean {
        val sec = nowMillis / 1000
        if (sec != windowSec) {
            windowSec = sec
            count = 0
        }
        if (count >= limitPerSecond) return false
        count++
        return true
    }
}
