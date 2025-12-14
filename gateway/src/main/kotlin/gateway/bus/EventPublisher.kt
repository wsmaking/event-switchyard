package gateway.bus

interface EventPublisher : AutoCloseable {
    fun publish(event: BusEvent)
    override fun close() {}
}

