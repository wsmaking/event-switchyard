package ai.assist.api;

import ai.assist.api.model.RiskNavigatorCheckRequest;
import ai.assist.api.model.RiskNavigatorCheckResponse;
import ai.assist.service.RiskNavigatorService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/ai/risk-navigator")
public class RiskNavigatorController {
    private final RiskNavigatorService service;

    public RiskNavigatorController(RiskNavigatorService service) {
        this.service = service;
    }

    @PostMapping("/check")
    public ResponseEntity<RiskNavigatorCheckResponse> check(@RequestBody RiskNavigatorCheckRequest request) {
        String requestId = "ai_" + UUID.randomUUID().toString().replace("-", "");
        return ResponseEntity.ok(service.check(requestId, request));
    }
}

