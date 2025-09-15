package app

import app.cdc.CdcQueue
import app.engine.Engine
import app.http.HttpIngress
import app.log.LogQueue
import app.log.LogWriter
import app.ownership.AssignmentPoller
import app.ownership.OwnedKeysView
import app.control.LeaseRegistrar
import io.etcd.jetcd.Client
import java.nio.file.Path
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicReference
import java.nio.charset.StandardCharsets.UTF_8
import io.etcd.jetcd.ByteSequence
import io.etcd.jetcd.options.GetOption

fun main() {
    val id = System.getenv("ENGINE_ID") ?: "e1"
    val endpoints = (System.getenv("ETCD_ENDPOINTS") ?: "http://127.0.0.1:2379")
        .split(",").filter { it.isNotBlank() }
    val client = Client.builder().endpoints(*endpoints.toTypedArray()).build()

    val ownedRef = AtomicReference<Set<String>>(emptySet())
    // envで担当を直指定（例: OWNED_KEYS="ABC,DEF"）
    System.getenv("OWNED_KEYS")?.takeIf { it.isNotBlank() }?.let {
        ownedRef.set(it.split(',').map(String::trim).filter { s -> s.isNotEmpty() }.toSet())
    }
    val ownership = OwnedKeysView(ownedRef)

    val logQ = LogQueue()
    val cdcQ = CdcQueue()
    val writer = LogWriter(logQ, Path.of("var/logs/append.log"))

    // etcd: Lease keepalive は専用クラスへ
    val leaseExec = Executors.newSingleThreadScheduledExecutor { r -> Thread(r, "lease-$id").apply{isDaemon=true} }
    val lease = LeaseRegistrar(client, id, scheduler = leaseExec)

    // etcd: assignments の fetch（prefixスキャンをisPrefix(true)で）
    val kv = client.kvClient
    val prefix = ByteSequence.from("/assignments/", UTF_8)
    val opt = GetOption.builder().isPrefix(true).build()
    val poller = AssignmentPoller(
        fetch = {
            val resp = kv.get(prefix, opt).get()
            resp.kvs.mapNotNull { kvp ->
                if (kvp.value.toString(UTF_8) == id) kvp.key.toString(UTF_8).substringAfterLast("/") else null
            }.toSet()
        },
        ownedRef = ownedRef,
        intervalMs = (System.getenv("ENGINE_POLL_MS") ?: "300").toLong()
    )

    val ingress = HttpIngress(Engine(ownership, logQ, cdcQ), 8080)

    Runtime.getRuntime().addShutdownHook(Thread { ingress.close(); poller.close(); lease.close(); writer.close(); client.close() })
}
