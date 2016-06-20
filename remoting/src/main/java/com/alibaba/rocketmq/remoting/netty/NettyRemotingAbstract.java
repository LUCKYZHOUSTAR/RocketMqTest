package com.alibaba.rocketmq.remoting.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.remoting.ChannelEventListener;
import com.alibaba.rocketmq.remoting.InvokeCallback;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.common.Pair;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import com.alibaba.rocketmq.remoting.common.ServiceThread;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;



/**
 * Server与Client公用的抽象类
 * @author LUCKY
 *
 */
public abstract class NettyRemotingAbstract {

    private static final Logger                                                    plog               = LoggerFactory
                                                                                                          .getLogger("RocketmqRemoting");
    // 信号量，Oneway情况会使用，防止本地Netty缓存请求过多
    protected final Semaphore                                                      semaphoreOneWay;
    // 信号量，异步调用情况会使用，防止本地Netty缓存请求过多
    protected final Semaphore                                                      semaphoreAsync;
    // 缓存所有对外请求
    protected final ConcurrentHashMap<Integer, ResponseFuture>                     responseTable      = new ConcurrentHashMap<Integer, ResponseFuture>(
                                                                                                          256);
    // 默认请求代码处理器
    protected Pair<NettyRequestProcessor, ExecutorService>                         defaultRequestProcessor;
    // 注册的各个RPC处理器
    protected final HashMap<Integer, Pair<NettyRequestProcessor, ExecutorService>> processorTable     = new HashMap(
                                                                                                          64);
    protected final NettyEventExecuter                                             nettyEventExecuter = new NettyEventExecuter();

    public abstract ChannelEventListener getChannelEventListener();

    public abstract RPCHook getRPCHook();

    public void putNettyEvent(NettyEvent event) {
        this.nettyEventExecuter.putNettyEvent(event);
    }

    public NettyRemotingAbstract(int permitsOneway, int permitsAsync) {
        this.semaphoreOneWay = new Semaphore(permitsOneway, true);
        this.semaphoreAsync = new Semaphore(permitsAsync, true);
    }

    public void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
        Pair matched = this.processorTable.get(Integer.valueOf(cmd.getCode()));
        final Pair pair = null == matched ? this.defaultRequestProcessor : matched;

        if (pair != null) {
            Runnable run = new Runnable() {
                public void run() {
                    try {
                        RPCHook rpcHook = NettyRemotingAbstract.this.getRPCHook();
                        //钩子函数，开始之前
                        if (rpcHook != null) {
                            rpcHook.doBeforeRequest(
                                RemotingHelper.parseChannelRemoteAddr(ctx.channel()), cmd);
                        }

                        //执行业务逻辑
                        RemotingCommand response = ((NettyRequestProcessor) pair.getObject1())
                            .processRequest(ctx, cmd);
                        //开始之后
                        if (rpcHook != null) {
                            rpcHook
                                .doAfterResponse(
                                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()), cmd,
                                    response);
                        }

                        if ((!cmd.isOnewayRPC()) && (response != null)) {
                            response.setOpaque(cmd.getOpaque());
                            response.markResponseType();
                            try {
                                ctx.writeAndFlush(response);
                            } catch (Throwable e) {
                                NettyRemotingAbstract.plog.error(
                                    "process request over, but response failed", e);
                                NettyRemotingAbstract.plog.error(cmd.toString());
                                NettyRemotingAbstract.plog.error(response.toString());
                            }
                        }
                    } catch (Exception e) {
                        NettyRemotingAbstract.plog.error("process request exception", e);
                        NettyRemotingAbstract.plog.error(cmd.toString());
                        if (!cmd.isOnewayRPC()) {
                            RemotingCommand response = RemotingCommand.createResponseCommand(1,
                                RemotingHelper.exceptionSimpleDesc(e));
                            response.setOpaque(cmd.getOpaque());
                            ctx.writeAndFlush(response);
                        }
                    }
                }
            };

            try {
                // 这里需要做流控，要求线程池对应的队列必须是有大小限制的
                ((ExecutorService) pair.getObject2()).submit(run);
            } catch (RejectedExecutionException e) {
                // 每个线程10s打印一次
                if (System.currentTimeMillis() % 10000L == 0L) {
                    plog.warn(RemotingHelper.parseChannelRemoteAddr(ctx.channel())
                              + ", too many requests and system thread pool busy, RejectedExecutionException "
                              + ((ExecutorService) pair.getObject2()).toString()
                              + " request code: " + cmd.getCode());
                }
                if (!cmd.isOnewayRPC()) {
                    RemotingCommand response = RemotingCommand.createResponseCommand(2,
                        "too many requests and system thread pool busy, please try another server");

                    response.setOpaque(cmd.getOpaque());
                    ctx.writeAndFlush(response);
                }
            }
        } else {
            String error = " request type" + cmd.getCode() + " not supported";
            RemotingCommand response = RemotingCommand.createResponseCommand(3, error);
            response.setOpaque(cmd.getOpaque());
            ctx.writeAndFlush(response);
            plog.error(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) + error);
        }

    }

    public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
        final ResponseFuture responseFuture = (ResponseFuture) this.responseTable.get(Integer
            .valueOf(cmd.getOpaque()));
        if (responseFuture != null) {
            responseFuture.setResponseCommand(cmd);
            responseFuture.release();
            this.responseTable.remove(Integer.valueOf(cmd.getOpaque()));
            if (responseFuture.getInvokeCallback() != null) {
                boolean runInThisThread = false;
                ExecutorService executor = getCallbackExecutor();
                if (executor != null) {
                    try {
                        executor.submit(new Runnable() {
                            public void run() {
                                try {
                                    responseFuture.executeInvokeCallback();
                                } catch (Throwable e) {
                                    NettyRemotingAbstract.plog
                                        .warn(
                                            "excute callback in executor exception, and callback throw",
                                            e);
                                }
                            }
                        });
                    } catch (Exception e) {
                        runInThisThread = true;
                        plog.warn("excute callback in executor exception, maybe executor busy", e);
                    }
                } else {
                    runInThisThread = true;

                }

                if (runInThisThread) {
                    try {
                        responseFuture.executeInvokeCallback();
                    } catch (Throwable e) {
                        plog.warn("executeInvokeCallback Exception", e);
                    }
                }
            } else {
                responseFuture.putResponse(cmd);
            }

        } else {
            plog.warn("receive response, but not matched any request, "
                      + RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

            plog.warn(cmd.toString());
        }

    }

    public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg)
                                                                                      throws Exception {
        RemotingCommand cmd = msg;
        if (cmd != null)
            switch (cmd.getType()) {
                case REQUEST_COMMAND:
                    processRequestCommand(ctx, cmd);
                    break;
                case RESPONSE_COMMAND:
                    processResponseCommand(ctx, cmd);
                    break;
            }
    }

    public void scanResponseTable() {
        Iterator it = this.responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry next = (Map.Entry) it.next();
            ResponseFuture rep = (ResponseFuture) next.getValue();
            if (rep.getBeginTimestamp() + rep.getTimeoutMillis() + 1000L <= System
                .currentTimeMillis()) {
                it.remove();
                try {
                    rep.executeInvokeCallback();
                } catch (Throwable e) {
                    plog.warn("scanResponseTable, operationComplete Exception", e);
                } finally {
                    rep.release();
                }
                plog.warn("remove timeout request, " + rep);
            }
        }
    }

    public RemotingCommand invokeSyncImpl(final Channel channel, final RemotingCommand request,
                                          long timeoutMillis) throws InterruptedException,
                                                             RemotingSendRequestException,
                                                             RemotingTimeoutException {
        try {
            final ResponseFuture responseFuture = new ResponseFuture(request.getOpaque(),
                timeoutMillis, null, null);

            this.responseTable.put(Integer.valueOf(request.getOpaque()), responseFuture);
            channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                public void operationComplete(ChannelFuture f) throws Exception {
                    if (f.isSuccess()) {
                        responseFuture.setSendRequestOK(true);
                        return;
                    }

                    responseFuture.setSendRequestOK(false);

                    NettyRemotingAbstract.this.responseTable.remove(Integer.valueOf(request
                        .getOpaque()));
                    responseFuture.setCause(f.cause());
                    responseFuture.putResponse(null);
                    NettyRemotingAbstract.plog.warn("send a request command to channel <"
                                                    + channel.remoteAddress() + "> failed.");
                    NettyRemotingAbstract.plog.warn(request.toString());
                }
            });
            RemotingCommand responseCommand = responseFuture.waitResponse(timeoutMillis);
            if (null == responseCommand) {
                if (responseFuture.isSendRequestOK()) {
                    throw new RemotingTimeoutException(
                        RemotingHelper.parseChannelRemoteAddr(channel), timeoutMillis,
                        responseFuture.getCause());
                }

                throw new RemotingSendRequestException(
                    RemotingHelper.parseChannelRemoteAddr(channel), responseFuture.getCause());
            }

            return responseCommand;
        } finally {
            this.responseTable.remove(Integer.valueOf(request.getOpaque()));
        }
    }

    public void invokeAsyncImpl(final Channel channel, final RemotingCommand request,
                                long timeoutMillis, InvokeCallback invokeCallback)
                                                                                  throws InterruptedException,
                                                                                  RemotingTooMuchRequestException,
                                                                                  RemotingTimeoutException,
                                                                                  RemotingSendRequestException {
        boolean acquired = this.semaphoreAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreAsync);

            final ResponseFuture responseFuture = new ResponseFuture(request.getOpaque(),
                timeoutMillis, invokeCallback, once);

            this.responseTable.put(Integer.valueOf(request.getOpaque()), responseFuture);
            try {
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    public void operationComplete(ChannelFuture f) throws Exception {
                        if (f.isSuccess()) {
                            responseFuture.setSendRequestOK(true);
                            return;
                        }

                        responseFuture.setSendRequestOK(false);

                        responseFuture.putResponse(null);
                        NettyRemotingAbstract.this.responseTable.remove(Integer.valueOf(request
                            .getOpaque()));
                        try {
                            responseFuture.executeInvokeCallback();
                        } catch (Throwable e) {
                            NettyRemotingAbstract.plog.warn(
                                "excute callback in writeAndFlush addListener, and callback throw",
                                e);
                        } finally {
                            responseFuture.release();
                        }

                        NettyRemotingAbstract.plog.warn(
                            "send a request command to channel <{}> failed.",
                            RemotingHelper.parseChannelRemoteAddr(channel));

                        NettyRemotingAbstract.plog.warn(request.toString());
                    }
                });
            } catch (Exception e) {
                responseFuture.release();
                plog.warn(
                    "send a request command to channel <"
                            + RemotingHelper.parseChannelRemoteAddr(channel) + "> Exception", e);

                throw new RemotingSendRequestException(
                    RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        } else {
            if (timeoutMillis <= 0L) {
                throw new RemotingTooMuchRequestException("invokeAsyncImpl invoke too fast");
            }

            String info = String
                .format(
                    "invokeAsyncImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d",
                    new Object[] { Long.valueOf(timeoutMillis),
                            Integer.valueOf(this.semaphoreAsync.getQueueLength()),
                            Integer.valueOf(this.semaphoreAsync.availablePermits()) });

            plog.warn(info);
            plog.warn(request.toString());
            throw new RemotingTimeoutException(info);
        }
    }

    public void invokeOnewayImpl(final Channel channel, final RemotingCommand request,
                                 long timeoutMillis) throws InterruptedException,
                                                    RemotingTooMuchRequestException,
                                                    RemotingTimeoutException,
                                                    RemotingSendRequestException {
        request.markOnewayRPC();
        boolean acquired = this.semaphoreOneWay.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreOneWay);
            try {
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    public void operationComplete(ChannelFuture f) throws Exception {
                        once.release();
                        if (!f.isSuccess()) {
                            NettyRemotingAbstract.plog.warn("send a request command to channel <"
                                                            + channel.remoteAddress() + "> failed.");

                            NettyRemotingAbstract.plog.warn(request.toString());
                        }
                    }
                });
            } catch (Exception e) {
                once.release();
                plog.warn("write send a request command to channel <" + channel.remoteAddress()
                          + "> failed.");
                throw new RemotingSendRequestException(
                    RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        } else {
            if (timeoutMillis <= 0L) {
                throw new RemotingTooMuchRequestException("invokeOnewayImpl invoke too fast");
            }

            String info = String
                .format(
                    "invokeOnewayImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d",
                    new Object[] { Long.valueOf(timeoutMillis),
                            Integer.valueOf(this.semaphoreAsync.getQueueLength()),
                            Integer.valueOf(this.semaphoreAsync.availablePermits()) });

            plog.warn(info);
            plog.warn(request.toString());
            throw new RemotingTimeoutException(info);
        }
    }

    public abstract ExecutorService getCallbackExecutor();

    class NettyEventExecuter extends ServiceThread {

        private final LinkedBlockingQueue<NettyEvent> eventQueue = new LinkedBlockingQueue<NettyEvent>();

        private final int                             MaxSize    = 10000;

        NettyEventExecuter() {
        }

        public void putNettyEvent(NettyEvent event) {
            if (this.eventQueue.size() <= 10000) {
                this.eventQueue.add(event);

            } else {
                NettyRemotingAbstract.plog.warn(
                    "event queue size[{}] enough, so drop this event {}",
                    Integer.valueOf(this.eventQueue.size()), event.toString());
            }
        }

        public void run() {
            plog.info(this.getServiceName() + " service started");

            final ChannelEventListener listener = NettyRemotingAbstract.this
                .getChannelEventListener();

            while (!this.isStoped()) {
                try {
                    NettyEvent event = this.eventQueue.poll(3000, TimeUnit.MILLISECONDS);
                    if (event != null && listener != null) {
                        switch (event.getType()) {
                            case IDLE:
                                listener.onChannelIdle(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CLOSE:
                                listener.onChannelClose(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CONNECT:
                                listener
                                    .onChannelConnect(event.getRemoteAddr(), event.getChannel());
                                break;
                            case EXCEPTION:
                                listener.onChannelException(event.getRemoteAddr(),
                                    event.getChannel());
                                break;
                            default:
                                break;

                        }
                    }
                } catch (Exception e) {
                    plog.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            plog.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return NettyEventExecuter.class.getSimpleName();
        }

    }
}
