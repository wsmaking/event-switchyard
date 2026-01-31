package gateway.order

import org.everit.json.schema.loader.SchemaLoader
import org.json.JSONObject
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path

class CreateOrderSchemaContractTest {
    private fun findSchema(): Path {
        var dir = Path.of("").toAbsolutePath()
        repeat(6) {
            val candidate = dir.resolve("contracts/order_request_v1.schema.json")
            if (Files.exists(candidate)) return candidate
            dir = dir.parent ?: return@repeat
        }
        throw IllegalStateException("schema not found")
    }

    @Test
    fun `schema accepts valid request`() {
        val schemaJson = JSONObject(Files.readString(findSchema()))
        val schema = SchemaLoader.load(schemaJson)
        val payload = JSONObject(
            mapOf(
                "symbol" to "BTC",
                "side" to "BUY",
                "type" to "LIMIT",
                "qty" to 1,
                "price" to 100,
                "timeInForce" to "GTC",
                "clientOrderId" to "c-1"
            )
        )

        assertDoesNotThrow { schema.validate(payload) }
    }

    @Test
    fun `schema rejects missing required fields`() {
        val schemaJson = JSONObject(Files.readString(findSchema()))
        val schema = SchemaLoader.load(schemaJson)
        val payload = JSONObject(mapOf("symbol" to "BTC"))

        assertThrows(Exception::class.java) { schema.validate(payload) }
    }
}
