package com.alibaba.rocketmq.remoting;

import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

public interface RPCHook {

    public void doBeforeRequest(String paramString, RemotingCommand paramRemotingCommand);

    public void doAfterResponse(String paramString, RemotingCommand paramRemotingCommand1,
                                RemotingCommand paramRemotingCommand2);

}
