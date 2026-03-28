package oms.order;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryOrderReadModelRecoveryTest {
    @TempDir
    Path tempDir;

    @Test
    void corruptStateFileIsBackedUpAndReset() throws Exception {
        Path statePath = tempDir.resolve("state.json");
        Files.writeString(statePath, "{\"orders\":[");
        System.setProperty("oms.state.path", statePath.toString());

        try {
            InMemoryOrderReadModel readModel = new InMemoryOrderReadModel("acct_demo");
            assertEquals(0, readModel.findAll().size());
            assertTrue(Files.exists(statePath));
            assertTrue(Files.list(tempDir).anyMatch(path -> path.getFileName().toString().startsWith("state.json.corrupt-")));
        } finally {
            System.clearProperty("oms.state.path");
        }
    }
}
