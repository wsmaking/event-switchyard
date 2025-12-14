package gateway.order

import java.util.concurrent.ConcurrentHashMap

class InMemoryOrderStore {
    private val byId = ConcurrentHashMap<String, OrderSnapshot>()
    private val idempotencyIndex = ConcurrentHashMap<String, String>() // idempotencyKey -> orderId

    fun findById(orderId: String): OrderSnapshot? = byId[orderId]

    fun findByIdempotencyKey(key: String): OrderSnapshot? {
        val orderId = idempotencyIndex[key] ?: return null
        return byId[orderId]
    }

    fun put(order: OrderSnapshot, idempotencyKey: String?) {
        byId[order.orderId] = order
        if (idempotencyKey != null) {
            idempotencyIndex.putIfAbsent(idempotencyKey, order.orderId)
        }
    }
}

