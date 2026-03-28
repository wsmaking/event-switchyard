package backofficejava.audit;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class FileAuditOffsetStore implements AuditOffsetStore {
    private final Path offsetPath;

    public FileAuditOffsetStore(Path offsetPath) {
        this.offsetPath = offsetPath;
    }

    @Override
    public long readOffset() {
        try {
            if (!Files.exists(offsetPath)) {
                return 0L;
            }
            String raw = Files.readString(offsetPath).trim();
            if (raw.isEmpty()) {
                return 0L;
            }
            return Long.parseLong(raw);
        } catch (Exception exception) {
            return 0L;
        }
    }

    @Override
    public void writeOffset(long offset) {
        try {
            Files.createDirectories(offsetPath.getParent());
            Files.writeString(
                offsetPath,
                Long.toString(offset),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
            );
        } catch (IOException exception) {
            throw new IllegalStateException("failed_to_write_backoffice_audit_offset:" + offsetPath, exception);
        }
    }

    @Override
    public String describe() {
        return offsetPath.toString();
    }
}
