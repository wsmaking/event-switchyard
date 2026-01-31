package gateway.order

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class CreateOrderRequestContractTest {
    private val mapper: ObjectMapper = ObjectMapper()
        .registerModule(KotlinModule.Builder().build())
        .registerModule(JavaTimeModule())

    @Test
    fun `missing type is rejected`() {
        val json = """{"symbol":"BTC","side":"BUY","qty":1,"price":100}"""

        assertThrows(Exception::class.java) {
            mapper.readValue(json, CreateOrderRequest::class.java)
        }
    }

    @Test
    fun `defaults are applied for timeInForce`() {
        val json = """{"symbol":"BTC","side":"BUY","type":"LIMIT","qty":1,"price":100}"""

        val request = mapper.readValue(json, CreateOrderRequest::class.java)

        assertEquals(TimeInForce.GTC, request.timeInForce)
        assertNull(request.clientOrderId)
    }
}
