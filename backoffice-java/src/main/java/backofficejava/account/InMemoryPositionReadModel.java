package backofficejava.account;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class InMemoryPositionReadModel implements PositionReadModel {
    private final String accountId;
    private final ConcurrentMap<String, List<PositionView>> positionsByAccount = new ConcurrentHashMap<>();

    public InMemoryPositionReadModel(String accountId) {
        this.accountId = accountId;
        reset();
    }

    @Override
    public List<PositionView> findByAccountId(String requestedAccountId) {
        return positionsByAccount.getOrDefault(requestedAccountId, List.of());
    }

    @Override
    public void replacePositions(String requestedAccountId, List<PositionView> positions) {
        positionsByAccount.put(requestedAccountId, List.copyOf(positions));
    }

    @Override
    public void reset() {
        positionsByAccount.clear();
        positionsByAccount.put(accountId, List.of());
    }
}
