package com.alibaba.rocketmq.remoting;

import io.netty.channel.Channel;

import java.util.concurrent.ExecutorService;

import com.alibaba.rocketmq.remoting.common.Pair;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

public interface RemotingServer extends RemotingService {
    public abstract void registerProcessor(int paramInt,
                                           NettyRequestProcessor paramNettyRequestProcessor,
                                           ExecutorService paramExecutorService);

    public abstract void registerDefaultProcessor(NettyRequestProcessor paramNettyRequestProcessor,
                                                  ExecutorService paramExecutorService);

    public abstract int localListenPort();

    public abstract Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(int paramInt);

    public abstract RemotingCommand invokeSync(Channel paramChannel,
                                               RemotingCommand paramRemotingCommand, long paramLong)
                                                                                                    throws InterruptedException,
                                                                                                    RemotingSendRequestException,
                                                                                                    RemotingTimeoutException;

    public abstract void invokeAsync(Channel paramChannel, RemotingCommand paramRemotingCommand,
                                     long paramLong, InvokeCallback paramInvokeCallback)
                                                                                        throws InterruptedException,
                                                                                        RemotingTooMuchRequestException,
                                                                                        RemotingTimeoutException,
                                                                                        RemotingSendRequestException;

    public abstract void invokeOneway(Channel paramChannel, RemotingCommand paramRemotingCommand,
                                      long paramLong) throws InterruptedException,
                                                     RemotingTooMuchRequestException,
                                                     RemotingTimeoutException,
                                                     RemotingSendRequestException;
}
