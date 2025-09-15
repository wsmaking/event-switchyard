@Get("/metrics")
fun metrics(): String = buildString {
  appendLine("# HELP ingress_qps ...")
  appendLine("ingress_qps ${Metrics.ingressQps.sumThenReset()}")
  appendLine("drop_count ${Metrics.dropCount.sum()}")
  appendLine("queue_depth ${Metrics.queueDepth.get()}")
  appendLine("apply_ns_p50 ${Metrics.applyNs.valueAtPercentile(50.0)}")
  appendLine("apply_ns_p95 ${Metrics.applyNs.valueAtPercentile(95.0)}")
  appendLine("apply_ns_p99 ${Metrics.applyNs.valueAtPercentile(99.0)}")
}