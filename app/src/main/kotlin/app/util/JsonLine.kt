package app.util
import com.fasterxml.jackson.module.afterburner.AfterburnerModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.nio.file.Files; import java.nio.file.Path; import java.nio.file.StandardOpenOption.*
object JsonLine {
    private val mapper = jacksonObjectMapper().registerModule(AfterburnerModule())
    fun write(path: Path, obj: Any) { Files.createDirectories(path.parent); Files.writeString(path, mapper.writeValueAsString(obj)+"\n", CREATE, WRITE, APPEND) }
}
