package com.alibaba.rocketmq.remoting;

public interface RemotingService {

    public void start();

    public void shutDown();

    public void registerRPCHook(RPCHook paramRPCHook);
}
