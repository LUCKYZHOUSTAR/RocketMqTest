package com.alibaba.rocketmq.remoting;

import com.alibaba.rocketmq.remoting.netty.ResponseFuture;

public interface InvokeCallback {

    public void operationComplete(ResponseFuture paramResponseFuture);
}
