package appjava.mobile;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class MobileLearningServiceTest {
    @Test
    void nullProgressIsDue() {
        assertTrue(MobileLearningService.isDue(null, 1_000L));
    }

    @Test
    void incompleteCardIsDue() {
        MobileProgressStore.CardProgress progress = new MobileProgressStore.CardProgress("card", false, false, 2, 1, 0, 100L);
        assertTrue(MobileLearningService.isDue(progress, 200L));
    }

    @Test
    void masteredCardWithinIntervalIsNotDue() {
        MobileProgressStore.CardProgress progress = new MobileProgressStore.CardProgress("card", false, true, 3, 3, 0, 1_000L);
        assertFalse(MobileLearningService.isDue(progress, 1_000L + 1_000L));
    }
}
