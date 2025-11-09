package app.marketdata

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import okhttp3.OkHttpClient
import okhttp3.Request
import java.util.concurrent.TimeUnit

class AlphaVantageProvider(private val apiKey: String) : MarketDataProvider {
    private val client = OkHttpClient.Builder()
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(10, TimeUnit.SECONDS)
        .build()

    private val mapper = jacksonObjectMapper()
    private val baseUrl = "https://www.alphavantage.co/query"

    override fun getQuote(symbol: String): Quote? {
        val url = "$baseUrl?function=GLOBAL_QUOTE&symbol=$symbol&apikey=$apiKey"
        val request = Request.Builder().url(url).build()

        return try {
            client.newCall(request).execute().use { response ->
                if (!response.isSuccessful) return null

                val json = mapper.readTree(response.body?.string())
                val quote = json.get("Global Quote") ?: return null

                Quote(
                    symbol = symbol,
                    price = quote.get("05. price")?.asDouble() ?: 0.0,
                    timestamp = System.currentTimeMillis(),
                    volume = quote.get("06. volume")?.asLong()
                )
            }
        } catch (e: Exception) {
            System.err.println("Error fetching quote for $symbol: ${e.message}")
            null
        }
    }

    override fun getHistoricalPrices(symbol: String, days: Int): List<Quote> {
        val url = "$baseUrl?function=TIME_SERIES_DAILY&symbol=$symbol&outputsize=compact&apikey=$apiKey"
        val request = Request.Builder().url(url).build()

        return try {
            client.newCall(request).execute().use { response ->
                if (!response.isSuccessful) return emptyList()

                val json = mapper.readTree(response.body?.string())
                val timeSeries = json.get("Time Series (Daily)") ?: return emptyList()

                timeSeries.fields().asSequence()
                    .take(days)
                    .map { (date, data) ->
                        Quote(
                            symbol = symbol,
                            price = data.get("4. close")?.asDouble() ?: 0.0,
                            timestamp = parseDate(date),
                            volume = data.get("5. volume")?.asLong()
                        )
                    }
                    .toList()
            }
        } catch (e: Exception) {
            System.err.println("Error fetching historical data for $symbol: ${e.message}")
            emptyList()
        }
    }

    private fun parseDate(dateStr: String): Long {
        return try {
            java.time.LocalDate.parse(dateStr)
                .atStartOfDay(java.time.ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli()
        } catch (e: Exception) {
            System.currentTimeMillis()
        }
    }
}
