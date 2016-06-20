package com.alibaba.rocketmq.remoting.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;

import java.net.SocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.remoting.ChannelEventListener;
import com.alibaba.rocketmq.remoting.InvokeCallback;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.RemotingClient;
import com.alibaba.rocketmq.remoting.common.Pair;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import com.alibaba.rocketmq.remoting.exception.RemotingConnectException;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

public class NettyRemotingClient extends NettyRemotingAbstract implements RemotingClient {

    private static final Logger                             log                = LoggerFactory
                                                                                   .getLogger("RocketmqRemoting");

    private static final long                               LockTimeoutMills   = 3000l;
    private final NettyClientConfig                         nettyClientConfig;
    private final Bootstrap                                 bootstrap          = new Bootstrap();
    private final EventLoopGroup                            eventLoopGroupWorker;
    private DefaultEventExecutorGroup                       defaultEventExecutorGroup;
    private final Lock                                      lockChannelTables  = new ReentrantLock();

    private final ConcurrentHashMap<String, ChannelWrapper> channelTables      = new ConcurrentHashMap();
    private final Timer                                     timer              = new Timer(
                                                                                   "ClientHouseKeepingService",
                                                                                   true);
    private final AtomicReference<List<String>>             namesrvAddrList    = new AtomicReference<List<String>>();
    private final AtomicReference<String>                   namesrvAddrChoosed = new AtomicReference<String>();
    private final AtomicInteger                             namesrvIndex       = new AtomicInteger(
                                                                                   initValueIndex());
    private final Lock                                      lockNamesrvChannel = new ReentrantLock();
    private final ExecutorService                           publicExecutor;
    private final ChannelEventListener                      channelEventListener;
    private RPCHook                                         rpcHook;

    private static int initValueIndex() {
        Random r = new Random();
        return Math.abs(r.nextInt() % 999) % 999;
    }

    public NettyRemotingClient(NettyClientConfig nettyClientConfig) {
        this(nettyClientConfig, null);
    }

    public NettyRemotingClient(NettyClientConfig nettyClientConfig,
                               ChannelEventListener channelEventListener) {
        super(nettyClientConfig.getClientOnewaySemaphoreValue(), nettyClientConfig
            .getClientAsyncSemaphoreValue());
        this.nettyClientConfig = nettyClientConfig;
        this.channelEventListener = channelEventListener;
        int publicThreadNums = nettyClientConfig.getClientCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }

        this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyClientPublicExecutor_"
                                     + this.threadIndex.incrementAndGet());
            }
        });

        this.eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);

            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("NettyClientSelector_%d",
                    new Object[] { Integer.valueOf(this.threadIndex.incrementAndGet()) }));
            }
        });

    }

    public void start() {
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
            this.nettyClientConfig.getClientWorkerThreads(), new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);

                public Thread newThread(Runnable r) {
                    return new Thread(r, "NettyClientWorkerThread_"
                                         + this.threadIndex.incrementAndGet());
                }
            });

        Bootstrap handler = this.bootstrap.group(this.eventLoopGroupWorker)
            .channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, false)
            .option(ChannelOption.SO_SNDBUF, nettyClientConfig.getClientSocketSndBufSize())
            .option(ChannelOption.SO_RCVBUF, nettyClientConfig.getClientSocketRcvBufSize())
            .handler(new ChannelInitializer<SocketChannel>() {

                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(
                        NettyRemotingClient.this.defaultEventExecutorGroup,
                        new ChannelHandler[] {
                                new NettyEncoder(),
                                new NettyDecoder(),
                                new IdleStateHandler(0, 0,
                                    NettyRemotingClient.this.nettyClientConfig
                                        .getClientChannelMaxIdleTimeSeconds()),
                                new NettyConnetManageHandler(), new NettyClientHandler() });

                }

            });

        this.timer.schedule(new TimerTask() {

            @Override
            public void run() {
                try {
                    NettyRemotingClient.this.scanResponseTable();

                } catch (Exception e) {
                    NettyRemotingClient.log.error("scanResponseTable exception", e);
                }

            }
        }, 3000l, 1000l);

        if (this.channelEventListener != null) {
            this.nettyEventExecuter.start();
        }
    }

    public void shutDown() {
        try {
            this.timer.cancel();
            for (ChannelWrapper cw : this.channelTables.values()) {
                closeChannel(null, cw.getChannel());
            }

            this.channelTables.clear();
            this.eventLoopGroupWorker.shutdownGracefully();
            if (this.nettyEventExecuter != null) {
                this.nettyEventExecuter.shutdown();
            }

            if (this.defaultEventExecutorGroup != null)
                this.defaultEventExecutorGroup.shutdownGracefully();
        } catch (Exception e) {
            log.error("NettyRemotingClient shutdown exception, ", e);
        }

        if (this.publicExecutor != null)
            try {
                this.publicExecutor.shutdown();
            } catch (Exception e) {
                log.error("NettyRemotingServer shutdown exception, ", e);
            }
    }

    private Channel getAndCreateChannel(String addr) throws InterruptedException {
        if (null == addr) {
            return getAndCreateNameserverChannel();
        }
        ChannelWrapper cw = (ChannelWrapper) this.channelTables.get(addr);
        if ((cw != null) && (cw.isOk())) {
            return cw.getChannel();
        }

        return createChannel(addr);
    }

    private Channel getAndCreateNameserverChannel() throws InterruptedException {
        String addr = (String) this.namesrvAddrChoosed.get();
        if (addr != null) {
            ChannelWrapper cw = (ChannelWrapper) this.channelTables.get(addr);
            if ((cw != null) && (cw.isOk())) {
                return cw.getChannel();
            }
        }

        List addrList = (List) this.namesrvAddrList.get();
        if (this.lockNamesrvChannel.tryLock(3000L, TimeUnit.MILLISECONDS)) {
            try {
                addr = (String) this.namesrvAddrChoosed.get();
                if (addr != null) {
                    ChannelWrapper cw = (ChannelWrapper) this.channelTables.get(addr);
                    if ((cw != null) && (cw.isOk())) {
                        return cw.getChannel();
                    }
                }

                if ((addrList != null) && (!addrList.isEmpty()))
                    for (int i = 0; i < addrList.size(); i++) {
                        int index = this.namesrvIndex.incrementAndGet();
                        index = Math.abs(index);
                        index %= addrList.size();
                        String newAddr = (String) addrList.get(index);

                        this.namesrvAddrChoosed.set(newAddr);
                        Channel channelNew = createChannel(newAddr);
                        if (channelNew != null)
                            return channelNew;
                    }
            } catch (Exception e) {
                log.error("getAndCreateNameserverChannel: create name server channel exception", e);
            } finally {
                this.lockNamesrvChannel.unlock();
            }
        } else {
            log.warn("getAndCreateNameserverChannel: try to lock name server, but timeout, {}ms",
                Long.valueOf(3000L));
        }

        return null;
    }

    private Channel createChannel(String addr) throws InterruptedException {
        ChannelWrapper cw = (ChannelWrapper) this.channelTables.get(addr);
        if ((cw != null) && (cw.isOk())) {
            return cw.getChannel();
        }

        if (this.lockChannelTables.tryLock(3000L, TimeUnit.MILLISECONDS)) {
            try {
                boolean createNewConnection = false;
                cw = (ChannelWrapper) this.channelTables.get(addr);
                if (cw != null) {
                    if (cw.isOk()) {
                        return cw.getChannel();
                    }

                    if (!cw.getChannelFuture().isDone()) {
                        createNewConnection = false;
                    } else {
                        this.channelTables.remove(addr);
                        createNewConnection = true;
                    }
                } else {
                    createNewConnection = true;
                }

                if (createNewConnection) {
                    ChannelFuture channelFuture = this.bootstrap.connect(RemotingHelper
                        .string2SocketAddress(addr));

                    log.info("createChannel: begin to connect remote host[{}] asynchronously", addr);
                    cw = new ChannelWrapper(channelFuture);
                    this.channelTables.put(addr, cw);
                }
            } catch (Exception e) {
                log.error("createChannel: create channel exception", e);
            } finally {
                this.lockChannelTables.unlock();
            }
        } else {
            log.warn("createChannel: try to lock channel table, but timeout, {}ms",
                Long.valueOf(3000L));
        }

        if (cw != null) {
            ChannelFuture channelFuture = cw.getChannelFuture();
            if (channelFuture
                .awaitUninterruptibly(this.nettyClientConfig.getConnectTimeoutMillis())) {
                if (cw.isOk()) {
                    log.info("createChannel: connect remote host[{}] success, {}", addr,
                        channelFuture.toString());

                    return cw.getChannel();
                }

                log.warn("createChannel: connect remote host[" + addr + "] failed, "
                         + channelFuture.toString(), channelFuture.cause());
            } else {
                log.warn("createChannel: connect remote host[{}] timeout {}ms, {}", new Object[] {
                        addr, Long.valueOf(this.nettyClientConfig.getConnectTimeoutMillis()),
                        channelFuture.toString() });
            }

        }

        return null;
    }

    public void closeChannel(String addr, Channel channel) {
        if (null == channel) {
            return;
        }
        String addrRemote = null == addr ? RemotingHelper.parseChannelRemoteAddr(channel) : addr;
        try {
            if (this.lockChannelTables.tryLock(3000L, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    ChannelWrapper prevCW = (ChannelWrapper) this.channelTables.get(addrRemote);

                    log.info("closeChannel: begin close the channel[{}] Found: {}", addrRemote,
                        Boolean.valueOf(prevCW != null));

                    if (null == prevCW) {
                        log.info(
                            "closeChannel: the channel[{}] has been removed from the channel table before",
                            addrRemote);

                        removeItemFromTable = false;
                    } else if (prevCW.getChannel() != channel) {
                        log.info(
                            "closeChannel: the channel[{}] has been closed before, and has been created again, nothing to do.",
                            addrRemote);

                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        this.channelTables.remove(addrRemote);
                        log.info("closeChannel: the channel[{}] was removed from channel table",
                            addrRemote);
                    }

                    RemotingUtil.closeChannel(channel);
                } catch (Exception e) {
                    log.error("closeChannel: close the channel exception", e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else
                log.warn("closeChannel: try to lock channel table, but timeout, {}ms",
                    Long.valueOf(3000L));
        } catch (InterruptedException e) {
            log.error("closeChannel exception", e);
        }
    }

    public void closeChannel(Channel channel) {
        if (null == channel)
            return;
        try {
            if (this.lockChannelTables.tryLock(3000L, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    ChannelWrapper prevCW = null;
                    String addrRemote = null;
                    for (String key : this.channelTables.keySet()) {
                        ChannelWrapper prev = (ChannelWrapper) this.channelTables.get(key);
                        if ((prev.getChannel() != null) && (prev.getChannel() == channel)) {
                            prevCW = prev;
                            addrRemote = key;
                            break;
                        }

                    }

                    if (null == prevCW) {
                        log.info(
                            "eventCloseChannel: the channel[{}] has been removed from the channel table before",
                            addrRemote);

                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        this.channelTables.remove(addrRemote);
                        log.info("closeChannel: the channel[{}] was removed from channel table",
                            addrRemote);
                        RemotingUtil.closeChannel(channel);
                    }
                } catch (Exception e) {
                    log.error("closeChannel: close the channel exception", e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else
                log.warn("closeChannel: try to lock channel table, but timeout, {}ms",
                    Long.valueOf(3000L));
        } catch (InterruptedException e) {
            log.error("closeChannel exception", e);
        }
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

    public RemotingCommand invokeSync(String addr, RemotingCommand request, long timeoutMillis)
                                                                                               throws InterruptedException,
                                                                                               RemotingConnectException,
                                                                                               RemotingSendRequestException,
                                                                                               RemotingTimeoutException {
        Channel channel = getAndCreateChannel(addr);
        if ((channel != null) && (channel.isActive())) {
            try {
                if (this.rpcHook != null) {
                    this.rpcHook.doBeforeRequest(addr, request);
                }
                RemotingCommand response = invokeSyncImpl(channel, request, timeoutMillis);
                if (this.rpcHook != null) {
                    this.rpcHook.doAfterResponse(RemotingHelper.parseChannelRemoteAddr(channel),
                        request, response);
                }

                return response;
            } catch (RemotingSendRequestException e) {
                log.warn("invokeSync: send request exception, so close the channel[{}]", addr);
                closeChannel(addr, channel);
                throw e;
            } catch (RemotingTimeoutException e) {
                log.warn("invokeSync: wait response timeout exception, the channel[{}]", addr);
                throw e;
            }
        }

        closeChannel(addr, channel);
        throw new RemotingConnectException(addr);
    }

    public void invokeAsync(String addr, RemotingCommand request, long timeoutMillis,
                            InvokeCallback invokeCallback) throws InterruptedException,
                                                          RemotingConnectException,
                                                          RemotingTooMuchRequestException,
                                                          RemotingTimeoutException,
                                                          RemotingSendRequestException {
        Channel channel = getAndCreateChannel(addr);
        if ((channel != null) && (channel.isActive())) {
            try {
                if (this.rpcHook != null) {
                    this.rpcHook.doBeforeRequest(addr, request);
                }
                invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);
            } catch (RemotingSendRequestException e) {
                log.warn("invokeAsync: send request exception, so close the channel[{}]", addr);
                closeChannel(addr, channel);
                throw e;
            }
        } else {
            closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    public void invokeOneway(String addr, RemotingCommand request, long timeoutMillis)
                                                                                      throws InterruptedException,
                                                                                      RemotingConnectException,
                                                                                      RemotingTooMuchRequestException,
                                                                                      RemotingTimeoutException,
                                                                                      RemotingSendRequestException {
        Channel channel = getAndCreateChannel(addr);
        if ((channel != null) && (channel.isActive())) {
            try {
                if (this.rpcHook != null) {
                    this.rpcHook.doBeforeRequest(addr, request);
                }
                invokeOnewayImpl(channel, request, timeoutMillis);
            } catch (RemotingSendRequestException e) {
                log.warn("invokeOneway: send request exception, so close the channel[{}]", addr);
                closeChannel(addr, channel);
                throw e;
            }
        } else {
            closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    public ExecutorService getCallbackExecutor() {
        return this.publicExecutor;
    }

    public void updateNameServerAddressList(List<String> addrs) {
        List old = (List) this.namesrvAddrList.get();
        boolean update = false;

        if (!addrs.isEmpty()) {
            if (null == old) {
                update = true;
            } else if (addrs.size() != old.size()) {
                update = true;
            } else {
                for (int i = 0; (i < addrs.size()) && (!update); i++) {
                    if (!old.contains(addrs.get(i))) {
                        update = true;
                    }
                }
            }

            if (update) {
                Collections.shuffle(addrs);
                this.namesrvAddrList.set(addrs);
            }
        }
    }

    public ChannelEventListener getChannelEventListener() {
        return this.channelEventListener;
    }

    public List<String> getNamesrvAddrList() {
        return (List) this.namesrvAddrList.get();
    }

    public List<String> getNameServerAddressList() {
        return (List) this.namesrvAddrList.get();
    }

    public RPCHook getRpcHook() {
        return this.rpcHook;
    }

    public void registerRPCHook(RPCHook rpcHook) {
        this.rpcHook = rpcHook;
    }

    public RPCHook getRPCHook() {
        return this.rpcHook;
    }

    public boolean isChannelWriteable(String addr) {
        ChannelWrapper cw = (ChannelWrapper) this.channelTables.get(addr);
        if ((cw != null) && (cw.isOk())) {
            return cw.isWriteable();
        }
        return true;
    }

    class ChannelWrapper {
        private final ChannelFuture channelFuture;

        public ChannelWrapper(ChannelFuture channelFuture) {
            this.channelFuture = channelFuture;
        }

        public boolean isOk() {
            return (this.channelFuture.channel() != null)
                   && (this.channelFuture.channel().isActive());
        }

        public boolean isWriteable() {
            return this.channelFuture.channel().isWritable();
        }

        Channel getChannel() {
            return this.channelFuture.channel();
        }

        public ChannelFuture getChannelFuture() {
            return this.channelFuture;
        }
    }

    class NettyConnetManageHandler extends ChannelDuplexHandler {
        NettyConnetManageHandler() {
        }

        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress,
                            SocketAddress localAddress, ChannelPromise promise) throws Exception {
            String local = localAddress == null ? "UNKNOW" : localAddress.toString();
            String remote = remoteAddress == null ? "UNKNOW" : remoteAddress.toString();
            NettyRemotingClient.log.info("NETTY CLIENT PIPELINE: CONNECT  {} => {}", local, remote);
            super.connect(ctx, remoteAddress, localAddress, promise);

            if (NettyRemotingClient.this.channelEventListener != null)
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT,
                    remoteAddress.toString(), ctx.channel()));
        }

        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            NettyRemotingClient.log.info("NETTY CLIENT PIPELINE: DISCONNECT {}", remoteAddress);
            NettyRemotingClient.this.closeChannel(ctx.channel());
            super.disconnect(ctx, promise);

            if (NettyRemotingClient.this.channelEventListener != null)
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE,
                    remoteAddress.toString(), ctx.channel()));
        }

        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            NettyRemotingClient.log.info("NETTY CLIENT PIPELINE: CLOSE {}", remoteAddress);
            NettyRemotingClient.this.closeChannel(ctx.channel());
            super.close(ctx, promise);

            if (NettyRemotingClient.this.channelEventListener != null)
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE,
                    remoteAddress.toString(), ctx.channel()));
        }

        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            NettyRemotingClient.log
                .warn("NETTY CLIENT PIPELINE: exceptionCaught {}", remoteAddress);
            NettyRemotingClient.log
                .warn("NETTY CLIENT PIPELINE: exceptionCaught exception.", cause);
            NettyRemotingClient.this.closeChannel(ctx.channel());
            if (NettyRemotingClient.this.channelEventListener != null)
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION,
                    remoteAddress.toString(), ctx.channel()));
        }

        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if ((evt instanceof IdleStateEvent)) {
                IdleStateEvent evnet = (IdleStateEvent) evt;
                if (evnet.state().equals(IdleState.ALL_IDLE)) {
                    String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    NettyRemotingClient.log.warn("NETTY CLIENT PIPELINE: IDLE exception [{}]",
                        remoteAddress);
                    NettyRemotingClient.this.closeChannel(ctx.channel());
                    if (NettyRemotingClient.this.channelEventListener != null) {
                        NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.IDLE,
                            remoteAddress.toString(), ctx.channel()));
                    }
                }

            }

            ctx.fireUserEventTriggered(evt);
        }
    }

    class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {
        NettyClientHandler() {
        }

        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg)
                                                                                   throws Exception {
            NettyRemotingClient.this.processMessageReceived(ctx, msg);
        }

    }
}
