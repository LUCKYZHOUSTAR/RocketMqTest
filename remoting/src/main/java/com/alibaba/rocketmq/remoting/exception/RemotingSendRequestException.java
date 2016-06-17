/**
 * 
 */
package com.alibaba.rocketmq.remoting.exception;

/** 
* @ClassName: RemotingSendRequestException 
* @Description: 
* @author LUCKY
* @date 2016年6月17日 下午1:46:41 
*  
*/
public class RemotingSendRequestException extends RemotingException {
    private static final long serialVersionUID = 5391285827332471674L;

    public RemotingSendRequestException(String addr) {
        this(addr, null);
    }

    public RemotingSendRequestException(String addr, Throwable cause) {
        super("send request to <" + addr + "> failed", cause);
    }
}
