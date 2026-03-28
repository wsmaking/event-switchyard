package backofficejava.account;

import java.util.List;

public interface FillReadModel {
    List<FillView> findByOrderId(String orderId);

    void replaceFills(String orderId, List<FillView> fills);

    void reset();
}
