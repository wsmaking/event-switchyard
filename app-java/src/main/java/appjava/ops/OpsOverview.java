package appjava.ops;

import appjava.clients.BackOfficeClient;
import appjava.clients.OmsClient;

import java.util.List;

public record OpsOverview(
    String accountId,
    String orderId,
    OmsClient.OmsStats omsStats,
    OmsClient.OmsReconcile omsReconcile,
    List<OmsClient.DeadLetterEntry> omsOrphans,
    BackOfficeClient.BackOfficeStats backOfficeStats,
    BackOfficeClient.BackOfficeReconcile backOfficeReconcile,
    List<BackOfficeClient.LedgerEntry> ledgerEntries,
    List<BackOfficeClient.DeadLetterEntry> backOfficeOrphans
) {
}
