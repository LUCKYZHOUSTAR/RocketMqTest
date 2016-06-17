/**
 * 
 */
package com.alibaba.rocketmq.remoting.exception;

/** 
* @ClassName: RemotingException 
* @Description: 异常的父类
* @author LUCKY
* @date 2016年6月17日 下午1:37:15 
*  
*/
public class RemotingException extends Exception {

    /** 
    * @Fields serialVersionUID : 
    */
    private static final long serialVersionUID = 111430129672849593L;

    public RemotingException(String message) {
        super(message);
    }

    public RemotingException(String message, Throwable cause) {
        super(message, cause);
    }

}
