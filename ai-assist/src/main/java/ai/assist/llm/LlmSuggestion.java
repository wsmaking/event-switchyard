package ai.assist.llm;

public record LlmSuggestion(
        String provider,
        String model,
        String text
) {}

