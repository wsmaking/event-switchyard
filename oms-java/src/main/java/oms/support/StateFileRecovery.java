package oms.support;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public final class StateFileRecovery {
    private StateFileRecovery() {
    }

    public static void recover(Path statePath, String label, IOException cause) {
        try {
            Files.createDirectories(statePath.getParent());
            if (Files.exists(statePath)) {
                Path backup = statePath.resolveSibling(statePath.getFileName() + ".corrupt-" + System.currentTimeMillis());
                Files.move(statePath, backup, StandardCopyOption.REPLACE_EXISTING);
                System.err.println("[warn] recovered corrupt " + label + " state: " + statePath + " -> " + backup + " cause=" + cause.getMessage());
            }
        } catch (IOException backupException) {
            cause.addSuppressed(backupException);
            throw new IllegalStateException("failed_to_recover_" + label + "_state:" + statePath, cause);
        }
    }
}
