package ai.assist.orchestrator;

public record OrchestrationResult(
        String suggestion,
        String source,
        boolean llmUsed,
        boolean fallbackUsed,
        String fallbackReason
) {}

