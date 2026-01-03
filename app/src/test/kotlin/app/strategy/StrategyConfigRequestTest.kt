package app.strategy

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class StrategyConfigRequestTest {
    @Test
    fun `validate rejects empty symbols and invalid ranges`() {
        val request =
            StrategyConfigRequest(
                enabled = true,
                symbols = listOf(" "),
                tickMs = 50,
                maxOrdersPerMin = -1,
                cooldownMs = 700_000
            )

        val errors = request.validate()
        assertTrue(errors.any { it.contains("symbols") })
        assertTrue(errors.any { it.contains("tickMs") })
        assertTrue(errors.any { it.contains("maxOrdersPerMin") })
        assertTrue(errors.any { it.contains("cooldownMs") })
    }

    @Test
    fun `validate accepts sane values`() {
        val request =
            StrategyConfigRequest(
                enabled = true,
                symbols = listOf("7203", "6758"),
                tickMs = 1000,
                maxOrdersPerMin = 10,
                cooldownMs = 1000
            )

        val errors = request.validate()
        assertFalse(errors.isNotEmpty())
    }
}
