package appjava.http;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

final class StaticFrontendHandlerTest {
    @Test
    void resolvesIndexForMobileRoute() {
        Path distDir = Path.of("/tmp/frontend-dist");
        Path resolved = StaticFrontendHandler.resolveRequestedFile(distDir, "/mobile/drills/drill-order-flow");
        assertEquals(distDir.resolve("index.html"), resolved);
    }

    @Test
    void resolvesStaticAsset() {
        Path distDir = Path.of("/tmp/frontend-dist");
        Path resolved = StaticFrontendHandler.resolveRequestedFile(distDir, "/assets/index-123.js");
        assertEquals(distDir.resolve("assets/index-123.js"), resolved);
    }

    @Test
    void rejectsTraversal() {
        Path distDir = Path.of("/tmp/frontend-dist");
        assertNull(StaticFrontendHandler.resolveRequestedFile(distDir, "/../secrets.txt"));
    }
}
