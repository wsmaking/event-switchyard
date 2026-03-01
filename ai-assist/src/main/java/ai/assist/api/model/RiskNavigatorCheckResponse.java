package ai.assist.api.model;

import java.util.List;

public record RiskNavigatorCheckResponse(
        String requestId,
        String decision,
        List<String> reasons,
        String suggestion,
        String suggestionSource,
        boolean llmUsed,
        boolean fallbackUsed,
        String fallbackReason,
        long processingMs
) {}

