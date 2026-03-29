package backofficejava.business;

import java.util.Optional;

public interface PostTradePackageReadModel {
    Optional<PostTradePackageView> findByOrderId(String orderId);

    void upsert(PostTradePackageView view);

    void reset();
}
