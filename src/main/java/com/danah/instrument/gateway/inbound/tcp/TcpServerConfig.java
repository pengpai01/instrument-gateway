package com.danah.instrument.gateway.inbound.tcp;

import com.danah.instrument.gateway.config.TcpProperties;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "gateway.tcp", name = "enabled", havingValue = "true", matchIfMissing = true)
public class TcpServerConfig implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(TcpServerConfig.class);

    private final TcpProperties properties;
    private final TcpFrameDecoder frameDecoder;
    private final TcpInboundHandler inboundHandler;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public TcpServerConfig(TcpProperties properties,
                           TcpFrameDecoder frameDecoder,
                           TcpInboundHandler inboundHandler) {
        this.properties = properties;
        this.frameDecoder = frameDecoder;
        this.inboundHandler = inboundHandler;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        bossGroup = new NioEventLoopGroup(properties.getBossThreads());
        workerGroup = new NioEventLoopGroup(properties.getWorkerThreads());
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast("frameDecoder", frameDecoder.newDecoder(properties.getMaxFrameBytes()))
                                .addLast("oversizedGuard", new OversizedFrameLogger())
                                .addLast("handler", inboundHandler);
                    }
                });

        serverChannel = bootstrap.bind(properties.getHost(), properties.getPort()).sync().channel();
        log.info("TCP ingest listening on {}:{} frameDecoder={} maxFrameBytes={}",
                properties.getHost(), properties.getPort(), frameDecoder.name(), properties.getMaxFrameBytes());
    }

    @PreDestroy
    public void shutdown() throws InterruptedException {
        if (serverChannel != null) {
            serverChannel.close().syncUninterruptibly();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        log.info("TCP ingest server stopped");
    }

    /**
     * Logs and closes when Netty rejects an oversized length-prefixed frame.
     */
    static final class OversizedFrameLogger extends io.netty.channel.ChannelInboundHandlerAdapter {
        private static final Logger log = LoggerFactory.getLogger(OversizedFrameLogger.class);

        @Override
        public void exceptionCaught(io.netty.channel.ChannelHandlerContext ctx, Throwable cause) {
            if (cause instanceof TooLongFrameException) {
                log.warn("TCP frame discarded (too long): {} — connection closed", cause.getMessage());
                ctx.close();
                return;
            }
            ctx.fireExceptionCaught(cause);
        }
    }
}
