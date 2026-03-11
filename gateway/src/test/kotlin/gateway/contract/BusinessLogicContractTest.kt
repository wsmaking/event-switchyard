package gateway.contract

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path

class BusinessLogicContractTest {
    private val mapper: ObjectMapper = jacksonObjectMapper()

    private fun findPath(relative: String): Path {
        var dir = Path.of("").toAbsolutePath()
        repeat(8) {
            val candidate = dir.resolve(relative)
            if (Files.exists(candidate)) return candidate
            dir = dir.parent ?: return@repeat
        }
        throw IllegalStateException("path not found: $relative")
    }

    private fun validateObjectBySchema(payload: ObjectNode, schema: JsonNode): List<String> {
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
                    value.isArray -> allowedTypes.contains("array")
                    value.isObject -> allowedTypes.contains("object")
                    else -> true
                }
                if (!typeOk) {
                    errors.add("type:$field")
                }
            }

            if (prop.has("enum")) {
                val enums = prop.path("enum").map {
                    if (it.isNull) "__NULL__" else it.asText()
                }.toSet()
                val actual = if (value.isNull) "__NULL__" else value.asText()
                if (!enums.contains(actual)) {
                    errors.add("enum:$field")
                }
            }

            if (prop.has("minimum") && value.isNumber) {
                val min = prop.path("minimum").asLong()
                if (value.asLong() < min) {
                    errors.add("min:$field")
                }
            }

            if (prop.has("maximum") && value.isNumber) {
                val max = prop.path("maximum").asLong()
                if (value.asLong() > max) {
                    errors.add("max:$field")
                }
            }
        }

        return errors
    }

    @Test
    fun `risk contract fixture validates and covers core reasons`() {
        val schema = mapper.readTree(Files.readString(findPath("contracts/risk_decision_v1.schema.json")))
        val payload = mapper.readTree(Files.readString(findPath("contracts/fixtures/risk_decision_v1.json"))) as ObjectNode
        val caseSchema = schema.path("properties").path("cases").path("items")
        val inputSchema = caseSchema.path("properties").path("input")
        val expectedSchema = caseSchema.path("properties").path("expected")

        val errors = mutableListOf<String>()
        errors += validateObjectBySchema(payload, schema)

        val cases = payload.path("cases")
        for (i in 0 until cases.size()) {
            val caseNode = cases.get(i)
            if (!caseNode.isObject) {
                errors.add("cases[$i]:not_object")
                continue
            }
            val caseObj = caseNode as ObjectNode
            errors += validateObjectBySchema(caseObj, caseSchema).map { "cases[$i].$it" }

            val inputNode = caseObj.path("input")
            if (!inputNode.isObject) {
                errors.add("cases[$i].input:not_object")
            } else {
                errors += validateObjectBySchema(inputNode as ObjectNode, inputSchema).map { "cases[$i].input.$it" }
            }

            val expectedNode = caseObj.path("expected")
            if (!expectedNode.isObject) {
                errors.add("cases[$i].expected:not_object")
            } else {
                errors +=
                    validateObjectBySchema(expectedNode as ObjectNode, expectedSchema).map { "cases[$i].expected.$it" }
            }
        }

        assertTrue(errors.isEmpty(), "errors=$errors")

        val reasons =
            (0 until cases.size())
                .mapNotNull { idx ->
                    val reason = cases.get(idx).path("expected").path("reason")
                    if (reason.isNull || reason.asText().isBlank()) null else reason.asText()
                }
                .toSet()
        assertTrue(
            reasons.containsAll(
                setOf("INVALID_QTY", "INVALID_SIDE", "INVALID_PRICE", "INVALID_SYMBOL", "RISK_REJECT")
            ),
            "covered reasons=$reasons"
        )
    }

    @Test
    fun `state transition contract fixture validates and keeps core invariants`() {
        val schema =
            mapper.readTree(Files.readString(findPath("contracts/order_state_transition_v1.schema.json")))
        val payload =
            mapper.readTree(Files.readString(findPath("contracts/fixtures/order_state_transition_v1.json"))) as ObjectNode
        val itemSchema = schema.path("properties").path("transitions").path("items")

        val errors = mutableListOf<String>()
        errors += validateObjectBySchema(payload, schema)

        val transitions = payload.path("transitions")
        val keys = mutableSetOf<String>()
        for (i in 0 until transitions.size()) {
            val node = transitions.get(i)
            if (!node.isObject) {
                errors.add("transitions[$i]:not_object")
                continue
            }
            val obj = node as ObjectNode
            errors += validateObjectBySchema(obj, itemSchema).map { "transitions[$i].$it" }

            val key = "${obj.path("from").asText()}|${obj.path("event").asText()}|${obj.path("to").asText()}"
            if (!keys.add(key)) {
                errors.add("transitions[$i]:duplicate_transition:$key")
            }
        }

        assertTrue(errors.isEmpty(), "errors=$errors")

        val transitionSet =
            (0 until transitions.size())
                .map { idx ->
                    val t = transitions.get(idx)
                    "${t.path("from").asText()}|${t.path("event").asText()}|${t.path("to").asText()}"
                }
                .toSet()

        val requiredTransitions = setOf(
            "ACCEPTED|CANCEL_REQUESTED|CANCEL_REQUESTED",
            "ACCEPTED|EXEC_REPORT_PARTIAL|PARTIALLY_FILLED",
            "PARTIALLY_FILLED|EXEC_REPORT_REJECTED|PARTIALLY_FILLED",
            "PARTIALLY_FILLED|EXEC_REPORT_FILLED|FILLED",
            "CANCELED|EXEC_REPORT_FILLED|CANCELED",
            "FILLED|EXEC_REPORT_CANCELED|FILLED"
        )
        assertTrue(transitionSet.containsAll(requiredTransitions), "transitions=$transitionSet")
    }

    @Test
    fun `reject reason contract fixture validates and keeps reason code uniqueness`() {
        val schema =
            mapper.readTree(Files.readString(findPath("contracts/reject_reason_v1.schema.json")))
        val payload = mapper.readTree(Files.readString(findPath("contracts/fixtures/reject_reason_v1.json"))) as ObjectNode
        val itemSchema = schema.path("properties").path("reasons").path("items")

        val errors = mutableListOf<String>()
        errors += validateObjectBySchema(payload, schema)

        val reasons = payload.path("reasons")
        val reasonSet = mutableSetOf<String>()
        val explicitCodeSet = mutableSetOf<Int>()
        for (i in 0 until reasons.size()) {
            val node = reasons.get(i)
            if (!node.isObject) {
                errors.add("reasons[$i]:not_object")
                continue
            }
            val obj = node as ObjectNode
            errors += validateObjectBySchema(obj, itemSchema).map { "reasons[$i].$it" }

            val reason = obj.path("reason").asText()
            if (!reasonSet.add(reason)) {
                errors.add("reasons[$i]:duplicate_reason:$reason")
            }

            val codeNode = obj.path("tcpReasonCode")
            if (!codeNode.isNull) {
                val code = codeNode.asInt()
                if (code != 9999 && !explicitCodeSet.add(code)) {
                    errors.add("reasons[$i]:duplicate_tcp_reason_code:$code")
                }
            }
        }

        assertTrue(errors.isEmpty(), "errors=$errors")

        val reasonToCode = HashMap<String, Int>()
        for (i in 0 until reasons.size()) {
            val node = reasons.get(i)
            reasonToCode[node.path("reason").asText()] = node.path("tcpReasonCode").asInt()
        }

        assertEquals(1001, reasonToCode["INVALID_QTY"])
        assertEquals(1002, reasonToCode["INVALID_SIDE"])
        assertEquals(1003, reasonToCode["INVALID_PRICE"])
        assertEquals(1004, reasonToCode["INVALID_SYMBOL"])
        assertEquals(1100, reasonToCode["RISK_REJECT"])
        assertEquals(2001, reasonToCode["V3_DURABLE_BACKPRESSURE_SOFT"])
        assertEquals(2002, reasonToCode["V3_DURABLE_BACKPRESSURE_HARD"])
        assertEquals(2101, reasonToCode["V3_BACKPRESSURE_SOFT"])
        assertEquals(2102, reasonToCode["V3_BACKPRESSURE_HARD"])
        assertEquals(2301, reasonToCode["V3_GLOBAL_KILLED"])
        assertEquals(2302, reasonToCode["V3_SESSION_KILLED"])
        assertEquals(2303, reasonToCode["V3_SHARD_KILLED"])
    }
}
