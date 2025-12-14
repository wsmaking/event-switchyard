package gateway.audit

interface AuditLog : AutoCloseable {
    fun append(event: AuditEvent)
    override fun close() {}
}

