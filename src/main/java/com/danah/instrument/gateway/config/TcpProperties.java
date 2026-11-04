package com.danah.instrument.gateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "gateway.tcp")
public class TcpProperties {

    private boolean enabled = true;
    private String host = "0.0.0.0";
    private int port = 9100;
    /** Entire frame including 2-byte length header must be &lt; this. */
    private int maxFrameBytes = 1024;
    private int bossThreads = 1;
    private int workerThreads = 2;
    /** Multi-frame reassembly: a record larger than one frame is split by the client. */
    private Assembly assembly = new Assembly();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getMaxFrameBytes() {
        return maxFrameBytes;
    }

    public void setMaxFrameBytes(int maxFrameBytes) {
        this.maxFrameBytes = maxFrameBytes;
    }

    public int getBossThreads() {
        return bossThreads;
    }

    public void setBossThreads(int bossThreads) {
        this.bossThreads = bossThreads;
    }

    public int getWorkerThreads() {
        return workerThreads;
    }

    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }

    public Assembly getAssembly() {
        return assembly;
    }

    public void setAssembly(Assembly assembly) {
        this.assembly = assembly;
    }

    /**
     * Bounds for merging length-prefixed fragments back into one record.
     * Separate from maxFrameBytes: a single frame stays small, the assembled record may be larger.
     */
    public static class Assembly {

        private boolean enabled = true;
        /** Max bytes of one fully assembled record. */
        private int maxBytes = 65536;
        /**
         * Max fragment count per record; also caps the part-array allocation.
         * Must stay large enough to carry {@link #maxBytes}: with 1024-byte frames one fragment
         * holds about 850 raw bytes (data is escaped text, no base64 inflation), so 128 fragments
         * cover the 64KB record limit with room to spare.
         */
        private int maxFragments = 128;
        /** Drop records still incomplete after this many milliseconds. */
        private int timeoutMs = 10000;
        /** Max records assembling at the same time per connection. */
        private int maxPendingPerChannel = 8;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getMaxBytes() {
            return maxBytes;
        }

        public void setMaxBytes(int maxBytes) {
            this.maxBytes = maxBytes;
        }

        public int getMaxFragments() {
            return maxFragments;
        }

        public void setMaxFragments(int maxFragments) {
            this.maxFragments = maxFragments;
        }

        public int getTimeoutMs() {
            return timeoutMs;
        }

        public void setTimeoutMs(int timeoutMs) {
            this.timeoutMs = timeoutMs;
        }

        public int getMaxPendingPerChannel() {
            return maxPendingPerChannel;
        }

        public void setMaxPendingPerChannel(int maxPendingPerChannel) {
            this.maxPendingPerChannel = maxPendingPerChannel;
        }
    }
}
