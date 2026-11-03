package com.danah.instrument.gateway.inbound.http;

import com.danah.instrument.gateway.inbound.IngestException;
import com.danah.instrument.gateway.inbound.IngestService;
import com.danah.instrument.gateway.inbound.model.InboundEvent;
import com.danah.instrument.gateway.inbound.model.IngestRequest;
import com.danah.instrument.gateway.inbound.model.Protocol;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/v1")
public class IngestController {

    private final IngestService ingestService;

    public IngestController(IngestService ingestService) {
        this.ingestService = ingestService;
    }

    @PostMapping("/ingest")
    public ResponseEntity<Map<String, Object>> ingest(@RequestBody IngestRequest body) {
        InboundEvent event = ingestService.ingest(body, Protocol.HTTP, null);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("eventId", event.getEventId());
        resp.put("deviceId", event.getDeviceId());
        resp.put("type", event.getType().name());
        resp.put("receivedAt", event.getReceivedAt().toString());
        return ResponseEntity.accepted().body(resp);
    }
}
