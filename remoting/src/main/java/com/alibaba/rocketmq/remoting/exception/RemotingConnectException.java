/**
 * 
 */
package com.alibaba.rocketmq.remoting.exception;

/** 
* @ClassName: RemotingConnectException 
* @Description: 
* @author LUCKY
* @date 2016年6月17日 下午1:38:20 
*  
*/
public class RemotingConnectException extends RemotingException {

    /** 
    * @Fields serialVersionUID : 
    */
    private static final long serialVersionUID = -7933705197488094535L;

    public RemotingConnectException(String addr) {
        this(addr, null);
    }

    public RemotingConnectException(String addr, Throwable cause) {
        super("connect to <" + addr + "> failed", cause);
    }
}
