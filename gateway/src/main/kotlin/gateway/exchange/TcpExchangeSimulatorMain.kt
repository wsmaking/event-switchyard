package gateway.exchange

object TcpExchangeSimulatorMain {
    @JvmStatic
    fun main(args: Array<String>) {
        // 単体プロセスとしてTCP取引所シミュレータを起動するエントリポイント。
        val sim = TcpExchangeSimulator()
        Runtime.getRuntime().addShutdownHook(Thread { sim.close() })
        sim.start()
        while (true) {
            Thread.sleep(1_000)
        }
    }
}
