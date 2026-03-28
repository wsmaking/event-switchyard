package backofficejava.account;

import java.util.List;

public interface PositionReadModel {
    List<PositionView> findByAccountId(String accountId);

    void replacePositions(String accountId, List<PositionView> positions);

    void reset();
}
