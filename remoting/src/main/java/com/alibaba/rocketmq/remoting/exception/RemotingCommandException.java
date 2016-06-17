/**
 * 
 */
package com.alibaba.rocketmq.remoting.exception;

/** 
* @ClassName: RemotingCommandException 
* @Description: 
* @author LUCKY
* @date 2016年6月17日 下午1:43:09 
*  
*/
public class RemotingCommandException extends RemotingException {
    private static final long serialVersionUID = -6061365915274953096L;

    public RemotingCommandException(String message) {
        super(message, null);
    }

    public RemotingCommandException(String message, Throwable cause) {
        super(message, cause);
    }
}
