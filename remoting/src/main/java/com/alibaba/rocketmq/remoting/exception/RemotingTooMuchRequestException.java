/**
 * 
 */
package com.alibaba.rocketmq.remoting.exception;

/** 
* @ClassName: RemotingTooMuchRequestException 
* @Description: 
* @author LUCKY
* @date 2016年6月17日 下午1:47:15 
*  
*/
public class RemotingTooMuchRequestException extends RemotingException {
    private static final long serialVersionUID = 4326919581254519654L;

    public RemotingTooMuchRequestException(String message) {
        super(message);
    }
}
