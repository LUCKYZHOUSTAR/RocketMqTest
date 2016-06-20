package com.alibaba.rocketmq.remoting.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;

import java.net.InetSocketAddress;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.remoting.ChannelEventListener;
import com.alibaba.rocketmq.remoting.InvokeCallback;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.RemotingServer;
import com.alibaba.rocketmq.remoting.common.Pair;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

public class NettyRemotingServer extends NettyRemotingAbstract implements RemotingServer {
    private static final Logger        log   = LoggerFactory.getLogger("RocketmqRemoting");
    private final ServerBootstrap      serverBootstrap;
    private final EventLoopGroup       eventLoopGroupWorker;
    private final EventLoopGroup       eventLoopGroupBoss;
    private final NettyServerConfig    nettyServerConfig;
    private final ExecutorService      publicExecutor;
    private final ChannelEventListener channelEventListener;
    private final Timer                timer = new Timer("ServerHouseKeepingService", true);
    private DefaultEventExecutorGroup  defaultEventExecutorGroup;
    private RPCHook                    rpcHook;
    private int                        port  = 0;

    public NettyRemotingServer(NettyServerConfig nettyServerConfig) {
        this(nettyServerConfig, null);
    }

    public NettyRemotingServer(final NettyServerConfig nettyServerConfig,
                               ChannelEventListener channelEventListener) {
        super(nettyServerConfig.getServerOnewaySemaphoreValue(), nettyServerConfig
            .getServerAsyncSemaphoreValue());

        this.serverBootstrap = new ServerBootstrap();
        this.nettyServerConfig = nettyServerConfig;
        this.channelEventListener = channelEventListener;

        int publicThreadNums = nettyServerConfig.getServerCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }

        this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyServerPublicExecutor_"
                                     + this.threadIndex.incrementAndGet());
            }
        });
        this.eventLoopGroupBoss = new NioEventLoopGroup(1, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettyBossSelector_%d",
                    new Object[] { Integer.valueOf(this.threadIndex.incrementAndGet()) }));
            }
        });
        this.eventLoopGroupWorker = new NioEventLoopGroup(
            nettyServerConfig.getServerSelectorThreads(), new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);
                private int           threadTotal = nettyServerConfig.getServerSelectorThreads();

                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format(
                        "NettyServerSelector_%d_%d",
                        new Object[] { Integer.valueOf(this.threadTotal),
                                Integer.valueOf(this.threadIndex.incrementAndGet()) }));
                }
            });
    }

    public void start() {
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
            this.nettyServerConfig.getServerWorkerThreads(), new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);

                public Thread newThread(Runnable r) {
                    return new Thread(r, "NettyServerWorkerThread_"
                                         + this.threadIndex.incrementAndGet());
                }
            });
        ServerBootstrap childHandler = this.serverBootstrap
            .group(this.eventLoopGroupBoss, this.eventLoopGroupWorker)
            .channel(NioServerSocketChannel.class)
            .option(ChannelOption.SO_BACKLOG, Integer.valueOf(1024))
            .option(ChannelOption.SO_REUSEADDR, Boolean.valueOf(true))
            .option(ChannelOption.SO_KEEPALIVE, Boolean.valueOf(false))
            .childOption(ChannelOption.TCP_NODELAY, Boolean.valueOf(true))
            .option(ChannelOption.SO_SNDBUF,
                Integer.valueOf(this.nettyServerConfig.getServerSocketSndBufSize()))
            .option(ChannelOption.SO_RCVBUF,
                Integer.valueOf(this.nettyServerConfig.getServerSocketRcvBufSize()))
            .localAddress(new InetSocketAddress(this.nettyServerConfig.getListenPort()))
            .childHandler(new ChannelInitializer() {
                @Override
                protected void initChannel(Channel ch) throws Exception {
                    ch.pipeline().addLast(
                        NettyRemotingServer.this.defaultEventExecutorGroup,
                        new ChannelHandler[] {
                                new NettyEncoder(),
                                new NettyDecoder(),
                                new IdleStateHandler(0, 0,
                                    NettyRemotingServer.this.nettyServerConfig
                                        .getServerChannelMaxIdleTimeSeconds()),
                                new NettyRemotingServer.NettyConnetManageHandler(),
                                new NettyRemotingServer.NettyServerHandler() });
                }

            });
        if (this.nettyServerConfig.isServerPooledByteBufAllocatorEnable()) {
            childHandler.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        }
        try {
            ChannelFuture sync = this.serverBootstrap.bind().sync();
            InetSocketAddress addr = (InetSocketAddress) sync.channel().localAddress();
            this.port = addr.getPort();
        } catch (InterruptedException e1) {
            throw new RuntimeException("this.serverBootstrap.bind().sync() InterruptedException",
                e1);
        }

        if (this.channelEventListener != null) {
            this.nettyEventExecuter.start();
        }

        this.timer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                try {
                    NettyRemotingServer.this.scanResponseTable();
                } catch (Exception e) {
                    NettyRemotingServer.log.error("scanResponseTable exception", e);
                }
            }
        }, 3000L, 1000L);
    }

    public void registerProcessor(int requestCode, NettyRequestProcessor processor,
                                  ExecutorService executor) {
        ExecutorService executorThis = executor;
        if (null == executor) {
            executorThis = this.publicExecutor;
        }

        Pair pair = new Pair(processor, executorThis);

        this.processorTable.put(Integer.valueOf(requestCode), pair);
    }

    public void registerDefaultProcessor(NettyRequestProcessor processor, ExecutorService executor) {
        this.defaultRequestProcessor = new Pair(processor, executor);
    }

    public RemotingCommand invokeSync(Channel channel, RemotingCommand request, long timeoutMillis)
                                                                                                   throws InterruptedException,
                                                                                                   RemotingSendRequestException,
                                                                                                   RemotingTimeoutException {
        return invokeSyncImpl(channel, request, timeoutMillis);
    }

    public void invokeAsync(Channel channel, RemotingCommand request, long timeoutMillis,
                            InvokeCallback invokeCallback) throws InterruptedException,
                                                          RemotingTooMuchRequestException,
                                                          RemotingTimeoutException,
                                                          RemotingSendRequestException {
        invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);
    }

    public void invokeOneway(Channel channel, RemotingCommand request, long timeoutMillis)
                                                                                          throws InterruptedException,
                                                                                          RemotingTooMuchRequestException,
                                                                                          RemotingTimeoutException,
                                                                                          RemotingSendRequestException {
        invokeOnewayImpl(channel, request, timeoutMillis);
    }

    public ChannelEventListener getChannelEventListener() {
        return this.channelEventListener;
    }

    public ExecutorService getCallbackExecutor() {
        return this.publicExecutor;
    }

    public void registerRPCHook(RPCHook rpcHook) {
        this.rpcHook = rpcHook;
    }

    public RPCHook getRPCHook() {
        return this.rpcHook;
    }

    public int localListenPort() {
        return this.port;
    }

    public Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(int requestCode) {
        return (Pair) this.processorTable.get(Integer.valueOf(requestCode));
    }

    class NettyConnetManageHandler extends ChannelDuplexHandler {
        NettyConnetManageHandler() {
        }

        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            NettyRemotingServer.log.info("NETTY SERVER PIPELINE: channelRegistered {}",
                remoteAddress);
            super.channelRegistered(ctx);
        }

        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            NettyRemotingServer.log.info(
                "NETTY SERVER PIPELINE: channelUnregistered, the channel[{}]", remoteAddress);
            super.channelUnregistered(ctx);
        }

        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            NettyRemotingServer.log.info("NETTY SERVER PIPELINE: channelActive, the channel[{}]",
                remoteAddress);
            super.channelActive(ctx);

            if (NettyRemotingServer.this.channelEventListener != null)
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT,
                    remoteAddress.toString(), ctx.channel()));
        }

        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            NettyRemotingServer.log.info("NETTY SERVER PIPELINE: channelInactive, the channel[{}]",
                remoteAddress);
            super.channelInactive(ctx);

            if (NettyRemotingServer.this.channelEventListener != null)
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE,
                    remoteAddress.toString(), ctx.channel()));
        }

        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if ((evt instanceof IdleStateEvent)) {
                IdleStateEvent evnet = (IdleStateEvent) evt;
                if (evnet.state().equals(IdleState.ALL_IDLE)) {
                    String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    NettyRemotingServer.log.warn("NETTY SERVER PIPELINE: IDLE exception [{}]",
                        remoteAddress);
                    RemotingUtil.closeChannel(ctx.channel());
                    if (NettyRemotingServer.this.channelEventListener != null) {
                        NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.IDLE,
                            remoteAddress.toString(), ctx.channel()));
                    }
                }

            }

            ctx.fireUserEventTriggered(evt);
        }

        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            NettyRemotingServer.log
                .warn("NETTY SERVER PIPELINE: exceptionCaught {}", remoteAddress);
            NettyRemotingServer.log
                .warn("NETTY SERVER PIPELINE: exceptionCaught exception.", cause);

            if (NettyRemotingServer.this.channelEventListener != null) {
                NettyRemotingServer.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION,
                    remoteAddress.toString(), ctx.channel()));
            }

            RemotingUtil.closeChannel(ctx.channel());
        }
    }

    class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {
        NettyServerHandler() {
        }

        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg)
                                                                                   throws Exception {
            NettyRemotingServer.this.processMessageReceived(ctx, msg);
        }
    }

    public void shutDown() {
        try {
            if (this.timer != null) {
                this.timer.cancel();
            }

            this.eventLoopGroupBoss.shutdownGracefully();

            this.eventLoopGroupWorker.shutdownGracefully();

            if (this.nettyEventExecuter != null) {
                this.nettyEventExecuter.shutdown();
            }

            if (this.defaultEventExecutorGroup != null)
                this.defaultEventExecutorGroup.shutdownGracefully();
        } catch (Exception e) {
            log.error("NettyRemotingServer shutdown exception, ", e);
        }

        if (this.publicExecutor != null)
            try {
                this.publicExecutor.shutdown();
            } catch (Exception e) {
                log.error("NettyRemotingServer shutdown exception, ", e);
            }
    }
}