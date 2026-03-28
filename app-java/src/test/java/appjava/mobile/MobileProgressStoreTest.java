package appjava.mobile;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class MobileProgressStoreTest {
    @Test
    void persistsBookmarkAndAnchor() throws Exception {
        Path tempDir = Files.createTempDirectory("mobile-progress-store");
        System.setProperty("app.state.dir", tempDir.toString());
        try {
            MobileProgressStore store = new MobileProgressStore("acct_test");
            store.applyAnchor("/mobile/orders/order-1", "order-1", null);
            store.setBookmark("aggregate-seq", true);

            MobileProgressStore reloaded = new MobileProgressStore("acct_test");
            MobileProgressStore.ProgressSnapshot snapshot = reloaded.snapshot();
            assertEquals("/mobile/orders/order-1", snapshot.anchor().route());
            assertEquals("order-1", snapshot.anchor().orderId());
            assertTrue(snapshot.cards().get("aggregate-seq").bookmarked());
            assertEquals(0L, snapshot.cards().get("aggregate-seq").nextReviewAt());
        } finally {
            System.clearProperty("app.state.dir");
        }
    }

    @Test
    void persistsDrillAttempt() throws Exception {
        Path tempDir = Files.createTempDirectory("mobile-progress-store-drill");
        System.setProperty("app.state.dir", tempDir.toString());
        try {
            MobileProgressStore store = new MobileProgressStore("acct_test");
            store.applyDrillAttempt("drill-sequence-gap", 2, "gap を待つ理由を説明", "data:audio/webm;base64,AAAA");

            MobileProgressStore reloaded = new MobileProgressStore("acct_test");
            MobileProgressStore.DrillProgress drill = reloaded.snapshot().drills().get("drill-sequence-gap");
            assertNotNull(drill);
            assertEquals(1, drill.attemptCount());
            assertEquals(2, drill.lastClarityScore());
            assertEquals("gap を待つ理由を説明", drill.lastNote());
            assertEquals("data:audio/webm;base64,AAAA", drill.audioDataUrl());
            assertTrue(drill.nextReviewAt() > drill.lastAttemptAt());
        } finally {
            System.clearProperty("app.state.dir");
        }
    }
}
