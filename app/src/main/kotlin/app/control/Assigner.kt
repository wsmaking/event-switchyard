package app.control

import io.etcd.jetcd.Client
import io.etcd.jetcd.ByteSequence
import io.etcd.jetcd.options.GetOption
import java.io.File
import java.nio.charset.StandardCharsets.UTF_8

// 簡易 JSONL 出力ユーティリティ。
// map を簡易に JSON 文字列化して指定ファイルに追記する。
//（エスケープ等は最小限の実装）
private fun jsonLine(path: String, map: Map<String, Any?>) {
  val f = File(path)
  f.parentFile?.mkdirs()
  f.appendText(
    map.entries.joinToString(prefix = "{", postfix = "}\n", separator = ",") { (k, v) ->
      val s = when (v) {
        null -> "null"
        is Number, is Boolean -> "$v"
        else -> "\"${v.toString().replace("\"", "\\\"")}\""
      }
      "\"$k\":$s"
    }
  )
}

fun main(args: Array<String>) {
  // コマンドライン引数取得ヘルパー
  fun arg(flag: String) = args.indexOf(flag).let { i -> if (i >= 0 && i + 1 < args.size) args[i + 1] else null }

  // 1) 入力パラメータ・環境変数の読み取り
  // symbols ファイルパスは必須
  val symbolsPath = arg("--symbols") ?: run {
    System.err.println("Usage: --symbols <path>")
    return
  }

  // etcd エンドポイント（カンマ区切り。環境変数で上書き可）
  val endpoints = (System.getenv("ETCD_ENDPOINTS") ?: "http://127.0.0.1:2379")
    .split(",").map { it.trim() }.filter { it.isNotEmpty() }

  // 結果出力先と runId（環境変数で上書き可。runId が無ければ時刻ベースで生成）
  val resultsDir = System.getenv("RESULTS_DIR") ?: "results"
  val runId = System.getenv("RUN_ID")
    ?: java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")
      .format(java.time.LocalDateTime.now())
  val logFile = "$resultsDir/assigner-$runId.jsonl"

  // 2) etcd クライアント作成
  val client = Client.builder().endpoints(*endpoints.toTypedArray()).build()
  val kv = client.kvClient

  // 3) 登録済みエンジン集合を取得
  //    /registry/engines/ プレフィックス下のキーからエンジンIDを抽出してソート
  val regPrefix = ByteSequence.from("/registry/engines/", UTF_8)
  val engines = kv.get(regPrefix, GetOption.builder().isPrefix(true).build()).get()
    .kvs.map { it.key.toString(UTF_8).substringAfterLast("/") }.sorted()

  if (engines.isEmpty()) {
    System.err.println("No engines registered under /registry/engines/* . Start engine containers first.")
    return
  }

  // 4) シンボル一覧をファイルから読み込む（空行を無視）
  val symbols = File(symbolsPath).readLines().map { it.trim() }.filter { it.isNotEmpty() }

  // 5) 全量割当処理（現状は単純な mod ハッシュ。将来 HRW に置換予定）
  val start = System.nanoTime()
  jsonLine(
    logFile, mapOf(
      "ts_ns" to start,
      "event" to "rebalance_start",
      "run_id" to runId,
      "engines" to engines.joinToString(","),
      "symbols" to symbols.size
    )
  )

  val assignPrefix = "/assignments/"
  var cnt = 0

  // 各シンボルを順に割当て、必要に応じて軽いスリープで etcd の過負荷を避ける
  symbols.forEachIndexed { i, sym ->
    val owner = engines[i % engines.size]
    kv.put(
      ByteSequence.from(assignPrefix + sym, UTF_8),
      ByteSequence.from(owner, UTF_8)
    ).get()
    if (++cnt % 1000 == 0) Thread.sleep(5)
  }

  val end = System.nanoTime()
  jsonLine(
    logFile, mapOf(
      "ts_ns" to end,
      "event" to "rebalance_end",
      "run_id" to runId,
      "engines" to engines.joinToString(","),
      "written" to cnt
    )
  )

  println("ASSIGNER done. engines=$engines written=$cnt results=$logFile")
}
