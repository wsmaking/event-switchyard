package backofficejava.account;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryOrderProjectionStateStoreRecoveryTest {
    @TempDir
    Path tempDir;

    @Test
    void corruptStateFileIsBackedUpAndReset() throws Exception {
        Path statePath = tempDir.resolve("orders.json");
        Files.writeString(statePath, "{\"states\":[");
        System.setProperty("backoffice.order.state.path", statePath.toString());

        try {
            InMemoryOrderProjectionStateStore store = new InMemoryOrderProjectionStateStore();
            assertEquals(0, store.findByAccountId("acct_demo").size());
            assertTrue(Files.exists(statePath));
            assertTrue(Files.list(tempDir).anyMatch(path -> path.getFileName().toString().startsWith("orders.json.corrupt-")));
        } finally {
            System.clearProperty("backoffice.order.state.path");
        }
    }
}
