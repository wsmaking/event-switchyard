package backoffice

import backoffice.http.HttpBackOffice
import backoffice.kafka.BackOfficeConsumer
import backoffice.store.InMemoryBackOfficeStore

fun main() {
    val port = (System.getenv("BACKOFFICE_PORT") ?: "8082").toInt()

    val store = InMemoryBackOfficeStore()
    val consumer = BackOfficeConsumer(store)
    val server = HttpBackOffice(port = port, store = store)

    Runtime.getRuntime().addShutdownHook(Thread {
        server.close()
        consumer.close()
    })

    consumer.start()
    server.start()
}

