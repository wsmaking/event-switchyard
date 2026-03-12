package gateway.contract

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.math.BigInteger
import java.nio.file.Files
import java.nio.file.Path

class BusinessLogicContractTest {
    private val mapper: ObjectMapper = jacksonObjectMapper()
    private val v3MaxOrderQty = 100_000_000L
    private val v3MaxNotional = 1_000_000_000L

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

    private data class RiskOracleDecision(
        val allowed: Boolean,
        val reason: String?,
        val httpStatus: Int
    )

    private fun isValidV3Symbol(raw: String): Boolean {
        val symbol = raw.trim()
        if (symbol.isBlank() || symbol.length > 8) return false
        for (ch in symbol) {
            val up = ch.uppercaseChar()
            val ok =
                (up.code in 'A'.code..'Z'.code) ||
                    (up.code in '0'.code..'9'.code) ||
                    up == '_' ||
                    up == '-' ||
                    up == '.'
            if (!ok) return false
        }
        return true
    }

    private fun kotlinHotRiskOracle(input: JsonNode): RiskOracleDecision {
        val side = input.path("side").asText()
        if (side != "BUY" && side != "SELL") {
            return RiskOracleDecision(false, "INVALID_SIDE", 422)
        }

        val qty = input.path("qty").asLong()
        if (qty == 0L || qty > v3MaxOrderQty) {
            return RiskOracleDecision(false, "INVALID_QTY", 422)
        }

        val symbol = input.path("symbol").asText()
        if (!isValidV3Symbol(symbol)) {
            return RiskOracleDecision(false, "INVALID_SYMBOL", 422)
        }
        val strictSymbols = input.path("strictSymbols").asBoolean(false)
        val symbolKnown = input.path("symbolKnown").asBoolean(true)
        if (strictSymbols && !symbolKnown) {
            return RiskOracleDecision(false, "INVALID_SYMBOL", 422)
        }

        val orderType = input.path("orderType").asText().uppercase()
        val price = input.path("price").asLong()
        if (orderType == "LIMIT" && price == 0L) {
            return RiskOracleDecision(false, "INVALID_PRICE", 422)
        }

        val notional = BigInteger.valueOf(qty).multiply(BigInteger.valueOf(price))
        if (notional > BigInteger.valueOf(v3MaxNotional)) {
            return RiskOracleDecision(false, "RISK_REJECT", 422)
        }

        return RiskOracleDecision(true, null, 202)
    }

    private fun isTerminalStatus(status: String): Boolean {
        return status == "FILLED" || status == "CANCELED" || status == "REJECTED"
    }

    private fun kotlinStateTransitionOracle(
        from: String,
        event: String,
        filledQtyBefore: Long,
        filledQtyReported: Long,
        orderQty: Long
    ): String {
        val nextFilled = maxOf(filledQtyBefore, filledQtyReported)
        if (isTerminalStatus(from)) return from

        val reportStatus =
            when (event) {
                "CANCEL_REQUESTED" -> "CANCEL_REQUESTED"
                "EXEC_REPORT_PARTIAL" -> "PARTIALLY_FILLED"
                "EXEC_REPORT_FILLED" -> "FILLED"
                "EXEC_REPORT_CANCELED" -> "CANCELED"
                "EXEC_REPORT_REJECTED" -> "REJECTED"
                else -> from
            }

        return when (reportStatus) {
            "PARTIALLY_FILLED" -> if (nextFilled >= orderQty) "FILLED" else "PARTIALLY_FILLED"
            "FILLED" -> "FILLED"
            "CANCELED" -> "CANCELED"
            "REJECTED" -> if (nextFilled > 0) from else "REJECTED"
            else -> reportStatus
        }
    }

    private fun v3TcpReasonCodeOracle(reason: String?): Int {
        return when (reason) {
            null -> 0
            "INVALID_QTY" -> 1001
            "INVALID_SIDE" -> 1002
            "INVALID_PRICE" -> 1003
            "INVALID_SYMBOL" -> 1004
            "RISK_REJECT" -> 1100
            "V3_DURABLE_BACKPRESSURE_SOFT" -> 2001
            "V3_DURABLE_BACKPRESSURE_HARD" -> 2002
            "V3_BACKPRESSURE_SOFT" -> 2101
            "V3_BACKPRESSURE_HARD" -> 2102
            "V3_QUEUE_KILLED" -> 2201
            "V3_QUEUE_FULL" -> 2202
            "V3_INGRESS_CLOSED" -> 2203
            "V3_GLOBAL_KILLED" -> 2301
            "V3_SESSION_KILLED" -> 2302
            "V3_SHARD_KILLED" -> 2303
            "BAD_TOKEN_LEN" -> 101
            "BAD_SYMBOL" -> 102
            "BAD_SIDE" -> 103
            "BAD_TYPE" -> 104
            "BAD_TOKEN_UTF8" -> 105
            "AUTH_INVALID" -> 201
            "AUTH_EXPIRED" -> 202
            "AUTH_NOT_YET_VALID" -> 203
            "AUTH_INTERNAL" -> 204
            else -> 9999
        }
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
    fun `risk contract fixture matches kotlin hot risk oracle`() {
        val payload = mapper.readTree(Files.readString(findPath("contracts/fixtures/risk_decision_v1.json")))
        val cases = payload.path("cases")
        val mismatches = mutableListOf<String>()
        for (i in 0 until cases.size()) {
            val caseNode = cases.get(i)
            val id = caseNode.path("id").asText("case-$i")
            val actual = kotlinHotRiskOracle(caseNode.path("input"))
            val expected = caseNode.path("expected")
            val expectedReasonNode = expected.path("reason")
            val expectedReason = if (expectedReasonNode.isNull) null else expectedReasonNode.asText()
            val expectedAllowed = expected.path("allowed").asBoolean()
            val expectedStatus = expected.path("httpStatus").asInt()
            if (
                actual.allowed != expectedAllowed ||
                    actual.reason != expectedReason ||
                    actual.httpStatus != expectedStatus
            ) {
                mismatches.add(
                    "$id expected=($expectedAllowed,$expectedReason,$expectedStatus) actual=(${actual.allowed},${actual.reason},${actual.httpStatus})"
                )
            }
        }
        assertTrue(mismatches.isEmpty(), "mismatches=$mismatches")
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
    fun `state transition fixture matches kotlin execution report oracle`() {
        val payload =
            mapper.readTree(Files.readString(findPath("contracts/fixtures/order_state_transition_v1.json")))
        val transitions = payload.path("transitions")
        val mismatches = mutableListOf<String>()
        for (i in 0 until transitions.size()) {
            val node = transitions.get(i)
            val id = node.path("id").asText("tr-$i")
            val actual =
                kotlinStateTransitionOracle(
                    from = node.path("from").asText(),
                    event = node.path("event").asText(),
                    filledQtyBefore = node.path("filledQtyBefore").asLong(),
                    filledQtyReported = node.path("filledQtyReported").asLong(),
                    orderQty = node.path("orderQty").asLong()
                )
            val expected = node.path("to").asText()
            if (actual != expected) {
                mismatches.add("$id expected=$expected actual=$actual")
            }
        }
        assertTrue(mismatches.isEmpty(), "mismatches=$mismatches")
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

    @Test
    fun `reject reason fixture matches kotlin tcp reason code oracle`() {
        val payload = mapper.readTree(Files.readString(findPath("contracts/fixtures/reject_reason_v1.json")))
        val reasons = payload.path("reasons")
        val mismatches = mutableListOf<String>()
        for (i in 0 until reasons.size()) {
            val node = reasons.get(i)
            val reason = node.path("reason").asText()
            val expectedCode = node.path("tcpReasonCode").asInt()
            val actualCode = v3TcpReasonCodeOracle(reason)
            if (actualCode != expectedCode) {
                mismatches.add("$reason expected=$expectedCode actual=$actualCode")
            }
        }
        assertTrue(mismatches.isEmpty(), "mismatches=$mismatches")
        assertEquals(9999, v3TcpReasonCodeOracle("UNKNOWN_REASON"))
    }

    @Test
    fun `amend decision contract fixture validates and covers core outcomes`() {
        val schema = mapper.readTree(Files.readString(findPath("contracts/amend_decision_v1.schema.json")))
        val payload = mapper.readTree(Files.readString(findPath("contracts/fixtures/amend_decision_v1.json"))) as ObjectNode
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
                    val reasonNode = cases.get(idx).path("expected").path("reason")
                    if (reasonNode.isNull || reasonNode.asText().isBlank()) null else reasonNode.asText()
                }
                .toSet()
        assertTrue(
            reasons.containsAll(setOf("ORDER_FINAL", "INVALID_QTY", "INVALID_PRICE")),
            "covered reasons=$reasons"
        )

        val nextStatuses =
            (0 until cases.size())
                .map { idx -> cases.get(idx).path("expected").path("nextStatus").asText() }
                .toSet()
        assertTrue(
            nextStatuses.contains("AMEND_REQUESTED"),
            "next statuses should include AMEND_REQUESTED: $nextStatuses"
        )
    }

    @Test
    fun `tif policy fixture validates and includes IOC FOK`() {
        val schema = mapper.readTree(Files.readString(findPath("contracts/tif_policy_v1.schema.json")))
        val payload = mapper.readTree(Files.readString(findPath("contracts/fixtures/tif_policy_v1.json"))) as ObjectNode
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
                errors += validateObjectBySchema(expectedNode as ObjectNode, expectedSchema).map {
                    "cases[$i].expected.$it"
                }
            }
        }

        assertTrue(errors.isEmpty(), "errors=$errors")

        val tifs =
            (0 until cases.size())
                .map { idx -> cases.get(idx).path("input").path("timeInForce").asText() }
                .toSet()
        assertTrue(tifs.contains("IOC"), "timeInForce should include IOC: $tifs")
        assertTrue(tifs.contains("FOK"), "timeInForce should include FOK: $tifs")
    }

    @Test
    fun `position cap fixture validates and covers reject reasons`() {
        val schema = mapper.readTree(Files.readString(findPath("contracts/position_cap_v1.schema.json")))
        val payload = mapper.readTree(Files.readString(findPath("contracts/fixtures/position_cap_v1.json"))) as ObjectNode
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
                errors += validateObjectBySchema(expectedNode as ObjectNode, expectedSchema).map {
                    "cases[$i].expected.$it"
                }
            }
        }

        assertTrue(errors.isEmpty(), "errors=$errors")

        val reasons =
            (0 until cases.size())
                .mapNotNull { idx ->
                    val reasonNode = cases.get(idx).path("expected").path("reason")
                    if (reasonNode.isNull || reasonNode.asText().isBlank()) null else reasonNode.asText()
                }
                .toSet()
        assertTrue(
            reasons.containsAll(setOf("POSITION_LIMIT_EXCEEDED", "INVALID_QTY", "INVALID_SIDE")),
            "covered reasons=$reasons"
        )
    }
}
