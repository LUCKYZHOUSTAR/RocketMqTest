/**
 * 
 */
package com.alibaba.rocketmq.remoting.protocol;

import java.util.concurrent.atomic.AtomicInteger;

/** 
* @ClassName: RemotingCommand 
* @Description: Remoting模块中，服务器与客户端通过传递RemotingCommand来交互
* @author LUCKY
* @date 2016年6月17日 下午3:18:44 
*  
*/
public class RemotingCommand {

    public static String RemotingVersionKey = "rocketmq.remoting.version";
    private static volatile int ConfigVersion=-1;
    private static AtomicInteger RequestId=new AtomicInteger(0);
    
}
