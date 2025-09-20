package app.cdc
import app.engine.CdcEvent
import java.util.concurrent.ArrayBlockingQueue
class CdcQueue(capacity: Int = 65536) { private val q = ArrayBlockingQueue<CdcEvent>(capacity); fun offer(e: CdcEvent) { q.offer(e) } }
