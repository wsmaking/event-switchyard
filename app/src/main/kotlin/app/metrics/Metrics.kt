package app.metrics

import java.util.concurrent.atomic.AtomicLong
object Metrics {
  val ingressTotal = AtomicLong()
  val ok200Total   = AtomicLong()
  val notOwner409  = AtomicLong()
  val busy429      = AtomicLong()
  val droppedTotal = AtomicLong()
  @Volatile var logQueueDepth: Int = 0
}
