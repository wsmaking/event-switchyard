package app.http

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpHandler
import java.nio.charset.StandardCharsets

/**
 * フロントエンド静的ファイル配信 (クラスパスから読み込み)
 */
class StaticFileController : HttpHandler {

    override fun handle(exchange: HttpExchange) {
        try {
            var path = exchange.requestURI.path

            // ルートパスは index.html にリダイレクト
            if (path == "/" || path.isEmpty()) {
                path = "/index.html"
            }

            // クラスパスからリソースを読み込み
            val resourcePath = "static$path"
            val resource = javaClass.classLoader.getResourceAsStream(resourcePath)

            if (resource == null) {
                // ファイルが見つからない場合は index.html を返す (SPA routing対応)
                serveResource(exchange, "static/index.html")
                return
            }

            val contentType = getContentType(path)
            val bytes = resource.readBytes()

            exchange.responseHeaders.set("Content-Type", contentType)
            exchange.sendResponseHeaders(200, bytes.size.toLong())
            exchange.responseBody.use { it.write(bytes) }
        } catch (e: Exception) {
            send(exchange, 500, "Internal Server Error: ${e.message}")
        } finally {
            exchange.close()
        }
    }

    private fun serveResource(exchange: HttpExchange, resourcePath: String) {
        val resource = javaClass.classLoader.getResourceAsStream(resourcePath)
            ?: throw IllegalStateException("Resource not found: $resourcePath")

        val contentType = getContentType(resourcePath)
        val bytes = resource.readBytes()

        exchange.responseHeaders.set("Content-Type", contentType)
        exchange.sendResponseHeaders(200, bytes.size.toLong())
        exchange.responseBody.use { it.write(bytes) }
    }

    private fun getContentType(filename: String): String {
        return when {
            filename.endsWith(".html") -> "text/html; charset=utf-8"
            filename.endsWith(".js") -> "application/javascript; charset=utf-8"
            filename.endsWith(".css") -> "text/css; charset=utf-8"
            filename.endsWith(".json") -> "application/json; charset=utf-8"
            filename.endsWith(".png") -> "image/png"
            filename.endsWith(".jpg") || filename.endsWith(".jpeg") -> "image/jpeg"
            filename.endsWith(".svg") -> "image/svg+xml"
            filename.endsWith(".ico") -> "image/x-icon"
            else -> "application/octet-stream"
        }
    }

    private fun send(exchange: HttpExchange, status: Int, text: String) {
        val bytes = text.toByteArray(StandardCharsets.UTF_8)
        exchange.responseHeaders.set("Content-Type", "text/plain; charset=utf-8")
        exchange.sendResponseHeaders(status, bytes.size.toLong())
        exchange.responseBody.use { it.write(bytes) }
    }
}
