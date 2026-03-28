package backofficejava.audit;

public interface AuditOffsetStore {
    long readOffset();

    void writeOffset(long offset);

    String describe();
}
