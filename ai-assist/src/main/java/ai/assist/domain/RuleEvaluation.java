package ai.assist.domain;

import java.util.List;

public record RuleEvaluation(
        RiskDecision decision,
        List<String> reasons
) {}

