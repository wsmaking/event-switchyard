package appjava.ops;

import appjava.clients.BackOfficeClient;
import appjava.clients.OmsClient;

public record AuditReplayOverview(
    OmsClient.ReplayResult oms,
    BackOfficeClient.ReplayResult backOffice
) {
}
