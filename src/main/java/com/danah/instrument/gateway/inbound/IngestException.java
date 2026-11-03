package com.danah.instrument.gateway.inbound;

public class IngestException extends RuntimeException {

    private final int status;

    public IngestException(int status, String message) {
        super(message);
        this.status = status;
    }

    public int getStatus() {
        return status;
    }
}
