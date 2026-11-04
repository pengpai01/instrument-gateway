package com.danah.instrument.gateway.inbound.http;

import com.danah.instrument.gateway.auth.ApiKeyAuthFilter;
import com.danah.instrument.gateway.inbound.IngestService;
import com.danah.instrument.gateway.inbound.IngressContext;
import com.danah.instrument.gateway.inbound.model.InboundEvent;
import com.danah.instrument.gateway.inbound.model.IngestRequest;
import com.danah.instrument.gateway.inbound.model.Protocol;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/v1")
public class IngestController {

    /** Optional source label for clients whose body carries no deviceId. */
    static final String HEADER_CLIENT_ID = "X-Client-Id";

    private final IngestService ingestService;

    public IngestController(IngestService ingestService) {
        this.ingestService = ingestService;
    }

    @PostMapping("/ingest")
    public ResponseEntity<Map<String, Object>> ingest(
            @RequestBody IngestRequest body,
            @RequestHeader(value = HEADER_CLIENT_ID, required = false) String clientHeader,
            HttpServletRequest httpRequest) {
        Object credentialClientId = httpRequest.getAttribute(ApiKeyAuthFilter.ATTR_CLIENT_ID);
        IngressContext context = new IngressContext(
                clientHeader,
                credentialClientId != null ? credentialClientId.toString() : null,
                null,
                httpRequest.getRemoteAddr());

        InboundEvent event = ingestService.ingest(body, Protocol.HTTP, context);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("eventId", event.getEventId());
        resp.put("sourceId", event.getSourceId());
        resp.put("deviceId", event.getDeviceId());
        resp.put("type", event.getType().name());
        resp.put("receivedAt", event.getReceivedAt().toString());
        return ResponseEntity.accepted().body(resp);
    }
}
