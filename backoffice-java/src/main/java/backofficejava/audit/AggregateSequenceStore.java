package backofficejava.audit;

public interface AggregateSequenceStore {
    long readLastApplied(String aggregateId);

    void markApplied(String aggregateId, long aggregateSeq, String eventRef, long eventAt);

    int size();

    void reset();
}
