package appjava.ops;

import appjava.clients.BackOfficeClient;
import appjava.clients.OmsClient;

import java.util.List;

public record OpsOverview(
    String accountId,
    String orderId,
    OmsClient.OmsStats omsStats,
    OmsClient.OmsReconcile omsReconcile,
    BackOfficeClient.BackOfficeStats backOfficeStats,
    BackOfficeClient.BackOfficeReconcile backOfficeReconcile,
    List<BackOfficeClient.LedgerEntry> ledgerEntries
) {
}
