/**
 * 
 */
package com.alibaba.rocketmq.remoting.exception;

/** 
* @ClassName: RemotingTimeoutException 
* @Description: 
* @author LUCKY
* @date 2016年6月17日 下午1:44:12 
*  
*/
public class RemotingTimeoutException extends RemotingException {

    private static final long serialVersionUID = 811504499017919965L;

    public RemotingTimeoutException(String message) {
        super(message);
    }

    public RemotingTimeoutException(String addr, long timeoutmills) {
        this(addr, timeoutmills, null);
    }

    public RemotingTimeoutException(String addr, long timeoutmills, Throwable cause) {
        super("wait response on the channel<" + addr + "> timeout", cause);
    }

}
