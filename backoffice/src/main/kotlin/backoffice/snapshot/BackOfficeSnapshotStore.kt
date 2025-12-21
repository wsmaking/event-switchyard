package backoffice.snapshot

import com.fasterxml.jackson.databind.ObjectMapper
import backoffice.json.Json
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

class BackOfficeSnapshotStore(
    private val path: Path,
    private val mapper: ObjectMapper = Json.mapper
) {
    fun load(): BackOfficeSnapshot? {
        if (!Files.exists(path)) return null
        return try {
            val raw = Files.readString(path)
            if (raw.isBlank()) null else mapper.readValue(raw, BackOfficeSnapshot::class.java)
        } catch (_: Throwable) {
            null
        }
    }

    fun save(snapshot: BackOfficeSnapshot) {
        path.parent?.let { Files.createDirectories(it) }
        val json = mapper.writeValueAsString(snapshot)
        Files.writeString(
            path,
            json,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE
        )
    }
}
