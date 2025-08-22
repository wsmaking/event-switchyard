package engine

import io.etcd.jetcd.Client
import io.etcd.jetcd.ByteSequence
import io.etcd.jetcd.options.PutOption
import io.etcd.jetcd.options.GetOption
import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

private fun nowNs() = System.nanoTime()
private fun envInt(name: String, def: Int) = System.getenv(name)?.toIntOrNull() ?: def
private fun jsonLine(path: String, map: Map<String, Any?>) {
  val f = File(path); f.parentFile?.mkdirs()
  f.appendText(map.entries.joinToString(prefix="{", postfix="}\n", separator=",") { (k,v) ->
    val s = when(v){
      null -> "null"
      is Number, is Boolean -> "$v"
      else -> "\"${v.toString().replace("\"","\\\"")}\""
    }
    "\"$k\":$s"
  })
}

fun main(args: Array<String>) {
  fun arg(flag: String) = args.indexOf(flag).let { i -> if (i>=0 && i+1<args.size) args[i+1] else null }

  val id = arg("--id") ?: System.getenv("ENGINE_ID") ?: run {
    System.err.println("Usage: --id <engineId>"); return
  }
  val endpoints = (System.getenv("ETCD_ENDPOINTS") ?: "http://127.0.0.1:2379")
    .split(",").map { it.trim() }.filter { it.isNotEmpty() }
  val resultsDir = System.getenv("RESULTS_DIR") ?: "results"
  val runId = System.getenv("RUN_ID") ?: "default"
  val logFile = "$resultsDir/engine-$id.jsonl"

  // 可変ノブ（環境変数で調整）
  val pollMs       = envInt("ENGINE_POLL_MS", 300)   // assignments ポーリング間隔(ms)
  val quietMs      = envInt("ENGINE_QUIET_MS", 800)  // “静止”判定のサイレンス時間(ms)
  val leaseTtlSec  = envInt("LEASE_TTL_SEC", 15)     // Lease TTL (秒)
  val leasePingSec = envInt("LEASE_PING_SEC", 3)     // keepAliveOnce 間隔(秒)

  fun newScheduler(name: String): ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor { r -> Thread(r, name).apply { isDaemon = true } }

  val client = Client.builder().endpoints(*endpoints.toTypedArray()).build()
  val kv = client.kvClient
  val lease = client.leaseClient

  // --- Lease 登録（keepAliveOnce を定期実行 + 期限切れ自動回復） ---
  val leaseIdRef = AtomicLong( lease.grant(leaseTtlSec.toLong()).get().id )
  fun putRegistryWith(leaseId: Long) {
    kv.put(
      ByteSequence.from("/registry/engines/$id", UTF_8),
      ByteSequence.from("""{"id":"$id","ts":"${Instant.now()}"}""", UTF_8),
      PutOption.builder().withLeaseId(leaseId).build()
    ).get(2, TimeUnit.SECONDS)
  }
  putRegistryWith(leaseIdRef.get())

  val leasePinger = newScheduler("lease-ping-$id")
  leasePinger.scheduleWithFixedDelay({
    try {
      lease.keepAliveOnce(leaseIdRef.get()).get(2, TimeUnit.SECONDS)
    } catch (_: Throwable) {
      // 期限切れ/ネットワーク断など → 再grant＆registry再登録（次サイクルでの再試行も可）
      try {
        val newId = lease.grant(leaseTtlSec.toLong()).get().id
        leaseIdRef.set(newId)
        putRegistryWith(newId)
        System.err.println("[$id] lease re-granted and registry re-put")
      } catch (_: Throwable) { /* noop */ }
    }
  }, 0, leasePingSec.toLong(), TimeUnit.SECONDS)

  // --- assignments をポーリングで同期 ---
  val myKeys = mutableSetOf<String>()
  val lastChange = AtomicLong(0L)
  val lastReported = AtomicLong(0L)

  // “静止”検知 → apply_done を1行JSONで出力
  val reporter = newScheduler("engine-reporter-$id")
  reporter.scheduleAtFixedRate({
    val last = lastChange.get()
    if (last > lastReported.get() && nowNs() - last > TimeUnit.MILLISECONDS.toNanos(quietMs.toLong())) {
      lastReported.set(last)
      jsonLine(logFile, mapOf(
        "ts_ns" to nowNs(),
        "event" to "engine_apply_done",
        "engine" to id,
        "run_id" to runId,
        "keys_owned" to myKeys.size
      ))
    }
  }, 500, 200, TimeUnit.MILLISECONDS)

  // poller: /assignments/* を全取得し、自分宛のみ反映
  val poller = newScheduler("assignments-poll-$id")
  val prefix = ByteSequence.from("/assignments/", UTF_8)
  val opt = GetOption.builder().withPrefix(prefix).build()
  poller.scheduleAtFixedRate({
    try {
      val resp = kv.get(prefix, opt).get()
      val next = mutableSetOf<String>()
      for (kvp in resp.kvs) {
        val owner = kvp.value.toString(UTF_8)
        if (owner == id) {
          val sym = kvp.key.toString(UTF_8).substringAfterLast("/")
          next.add(sym)
        }
      }
      if (next != myKeys) {
        myKeys.clear()
        myKeys.addAll(next)
        lastChange.set(nowNs())
      }
    } catch (t: Throwable) {
      t.printStackTrace()
    }
  }, 0, pollMs.toLong(), TimeUnit.MILLISECONDS)

  Runtime.getRuntime().addShutdownHook(Thread {
    try { leasePinger.shutdownNow() } catch (_: Throwable) {}
    try { poller.shutdownNow() } catch (_: Throwable) {}
    try { reporter.shutdownNow() } catch (_: Throwable) {}
    try { client.close() } catch (_: Throwable) {}
  })

  println("ENGINE $id up. endpoints=$endpoints results=$logFile (polling mode)")
  while (true) Thread.sleep(2000)
}
