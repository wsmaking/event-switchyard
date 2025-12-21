package backoffice.snapshot

import backoffice.store.BackOfficeSnapshotState
import java.time.Instant

data class BackOfficeSnapshot(
    val createdAt: Instant,
    val ledgerLines: Long,
    val state: BackOfficeSnapshotState
)
