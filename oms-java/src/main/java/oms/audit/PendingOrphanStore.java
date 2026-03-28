package oms.audit;

import java.util.List;

public interface PendingOrphanStore {
    void append(PendingOrphanEntryView entry);

    List<PendingOrphanEntryView> find(String orderId, int limit);

    void removeByEventRef(String eventRef);

    int size();

    void reset();
}
