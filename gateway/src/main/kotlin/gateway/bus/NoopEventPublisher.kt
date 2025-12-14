package gateway.bus

object NoopEventPublisher : EventPublisher {
    override fun publish(event: BusEvent) {}
}

