package backoffice.contract

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path

class BusEventSchemaContractTest {
    private val mapper: ObjectMapper = jacksonObjectMapper()

    private fun findSchema(): Path {
        var dir = Path.of("").toAbsolutePath()
        repeat(6) {
            val candidate = dir.resolve("contracts/bus_event_v1.schema.json")
            if (Files.exists(candidate)) return candidate
            dir = dir.parent ?: return@repeat
        }
        throw IllegalStateException("schema not found")
    }

    private fun findFixture(): Path {
        var dir = Path.of("").toAbsolutePath()
        repeat(6) {
            val candidate = dir.resolve("contracts/fixtures/bus_event_v1.json")
            if (Files.exists(candidate)) return candidate
            dir = dir.parent ?: return@repeat
        }
        throw IllegalStateException("fixture not found")
    }

    private fun validate(payload: ObjectNode, schema: JsonNode): List<String> {
        val errors = mutableListOf<String>()
        val required = schema.path("required").map { it.asText() }.toSet()
        val properties = schema.path("properties")

        for (name in required) {
            if (!payload.has(name)) {
                errors.add("missing:$name")
            }
        }

        if (schema.path("additionalProperties").asBoolean(true).not()) {
            payload.fieldNames().forEachRemaining { field ->
                if (!properties.has(field)) {
                    errors.add("unexpected:$field")
                }
            }
        }

        payload.fieldNames().forEachRemaining { field ->
            val prop = properties.path(field)
            if (prop.isMissingNode) return@forEachRemaining
            val value = payload.get(field)

            val allowedTypes = prop.path("type").let { node ->
                when {
                    node.isArray -> node.map { it.asText() }
                    node.isTextual -> listOf(node.asText())
                    else -> emptyList()
                }
            }
            if (allowedTypes.isNotEmpty()) {
                val typeOk = when {
                    value.isNull -> allowedTypes.contains("null")
                    value.isIntegralNumber -> allowedTypes.contains("integer") || allowedTypes.contains("number")
                    value.isNumber -> allowedTypes.contains("number")
                    value.isTextual -> allowedTypes.contains("string")
                    value.isBoolean -> allowedTypes.contains("boolean")
                    value.isObject -> allowedTypes.contains("object")
                    else -> true
                }
                if (!typeOk) {
                    errors.add("type:$field")
                }
            }
        }

        return errors
    }

    @Test
    fun `schema accepts order accepted event`() {
        val schema = mapper.readTree(Files.readString(findSchema()))
        val payload = mapper.createObjectNode().apply {
            put("type", "OrderAccepted")
            put("at", "2025-01-01T00:00:00Z")
            put("accountId", "acct-1")
            put("orderId", "ord-1")
            set<ObjectNode>("data", mapper.createObjectNode().apply {
                put("symbol", "BTC")
                put("side", "BUY")
                put("type", "LIMIT")
                put("qty", 1)
                put("price", 100)
            })
        }

        val errors = validate(payload, schema)
        assertTrue(errors.isEmpty(), "errors=$errors")
    }

    @Test
    fun `schema accepts fixture payload`() {
        val schema = mapper.readTree(Files.readString(findSchema()))
        val payload = mapper.readTree(Files.readString(findFixture())) as ObjectNode
        val errors = validate(payload, schema)
        assertTrue(errors.isEmpty(), "errors=$errors")
    }

    @Test
    fun `schema rejects missing required fields`() {
        val schema = mapper.readTree(Files.readString(findSchema()))
        val payload = mapper.createObjectNode().apply {
            put("type", "OrderAccepted")
        }

        val errors = validate(payload, schema)
        assertFalse(errors.isEmpty())
    }
}
