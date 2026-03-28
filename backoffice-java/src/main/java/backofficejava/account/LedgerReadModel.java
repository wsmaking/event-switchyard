package backofficejava.account;

import java.util.List;

public interface LedgerReadModel {
    List<LedgerEntryView> find(String accountId, String orderId, String eventType, int limit, Long afterEventAt);

    List<LedgerEntryView> findByAccountId(String accountId);

    void append(LedgerEntryView entry);

    boolean containsEventRef(String eventRef);

    void reset();
}
