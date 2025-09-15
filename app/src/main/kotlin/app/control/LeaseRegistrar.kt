package app.control

import io.etcd.jetcd.Client
import io.etcd.jetcd.ByteSequence
import io.etcd.jetcd.options.PutOption
import java.time.Instant
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

class LeaseRegistrar(
    private val client: Client,
    private val engineId: String,
    private val ttlSec: Long = 15,
    private val pingSec: Long = 3,
    private val scheduler: ScheduledExecutorService
) : AutoCloseable {
    private val lease = client.leaseClient
    private val kv = client.kvClient
    private val leaseId = AtomicLong(0L) // 0 = 未登録（次tickで再試行）

    init {
        // 初回は非同期で試行（ブロックしない）
        scheduler.execute { tryGrantAndPut() }

        // keepAlive or 再grant を定期実行（失敗時は0に戻して再試行）
        scheduler.scheduleWithFixedDelay({
            try {
                val id = leaseId.get()
                if (id == 0L) {
                    tryGrantAndPut()
                } else {
                    lease.keepAliveOnce(id).get(2, TimeUnit.SECONDS)
                }
            } catch (_: Throwable) {
                leaseId.set(0L) // 次tickで再grant
            }
        }, pingSec, pingSec, TimeUnit.SECONDS)
    }

    private fun tryGrantAndPut() {
        try {
            val id = lease.grant(ttlSec).get(2, TimeUnit.SECONDS).id
            leaseId.set(id)
            kv.put(
                ByteSequence.from("/registry/engines/$engineId", UTF_8),
                ByteSequence.from("""{"id":"$engineId","ts":"${Instant.now()}"}""", UTF_8),
                PutOption.builder().withLeaseId(id).build()
            ).get(2, TimeUnit.SECONDS)
            println("LeaseRegistrar: registered /registry/engines/$engineId (lease=$id)")
        } catch (_: Throwable) {
            // etcd未起動でもOK。次tickで再試行します
        }
    }

    override fun close() { scheduler.shutdownNow() }
}
