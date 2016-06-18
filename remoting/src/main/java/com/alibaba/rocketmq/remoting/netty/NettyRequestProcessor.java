package com.alibaba.rocketmq.remoting.netty;

import io.netty.channel.ChannelHandlerContext;

import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

public interface NettyRequestProcessor {

    public RemotingCommand processRequest(ChannelHandlerContext paramChannelHandlerContext,
                                          RemotingCommand parRemotingCommand) throws Exception;
}
