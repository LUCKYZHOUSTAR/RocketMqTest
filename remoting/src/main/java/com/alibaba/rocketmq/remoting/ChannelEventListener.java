package com.alibaba.rocketmq.remoting;

import io.netty.channel.Channel;

public interface ChannelEventListener {

    public void onChannelConnect(String paramString, Channel paramChannel);

    public void onChannelClose(String paramString, Channel paramChannel);

    public void onChannelException(String paramString, Channel paramChannel);

    public void onChannelIdle(String paramString, Channel paramChannel);

}
