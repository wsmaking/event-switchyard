package ai.assist.policy;

import org.springframework.stereotype.Component;

@Component
public class ResponsePolicyEngine {
    public String apply(String suggestion) {
        String safe = suggestion == null ? "" : suggestion.trim();
        if (safe.isEmpty()) {
            safe = "情報不足のため、板・数量・価格条件を再確認してください。";
        }

        safe = safe
                .replace("必ず儲かる", "収益は保証されない")
                .replace("絶対に勝てる", "損失可能性を含む");

        if (!safe.contains("投資助言")) {
            safe = safe + "（本機能は投資助言ではありません）";
        }
        return safe;
    }
}

