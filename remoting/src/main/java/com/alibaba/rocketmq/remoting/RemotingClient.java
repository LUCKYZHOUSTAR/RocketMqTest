package com.alibaba.rocketmq.remoting;

import java.util.List;
import java.util.concurrent.ExecutorService;

import com.alibaba.rocketmq.remoting.exception.RemotingConnectException;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

public interface RemotingClient extends RemotingService {

    public void updateNameServerAddressList(List<String> paramList);

    public List<String> getNameServerAddressList();

    public RemotingCommand invokeSync(String paramString, RemotingCommand paramRemotingCommand,
                                      long paramLong) throws InterruptedException,
                                                     RemotingConnectException,
                                                     RemotingSendRequestException,
                                                     RemotingTimeoutException;

    public void invokeAsync(String paramString, RemotingCommand paramRemotingCommand,
                            long paramLong, InvokeCallback paramInvokeCallback)
                                                                               throws InterruptedException,
                                                                               RemotingConnectException,
                                                                               RemotingTooMuchRequestException,
                                                                               RemotingTimeoutException,
                                                                               RemotingSendRequestException;

    public void invokeOneway(String paramString, RemotingCommand paramRemotingCommand,
                             long paramLong) throws InterruptedException, RemotingConnectException,
                                            RemotingTooMuchRequestException,
                                            RemotingTimeoutException, RemotingSendRequestException;

    public void registerProcessor(int paramInt, NettyRequestProcessor paramNettyRequestProcessor,
                                  ExecutorService paramExecutorService);

    public boolean isChannelWriteable(String paramString);
}
