package oms.audit;

import java.util.List;

public interface DeadLetterStore {
    void append(DeadLetterEntryView entry);

    List<DeadLetterEntryView> find(String orderId, int limit);

    int size();

    DeadLetterEntryView findByEventRef(String eventRef);

    void removeByEventRef(String eventRef);

    void reset();
}
