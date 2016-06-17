/**
 * 
 */
package com.alibaba.rocketmq.remoting;

import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;

/** 
* @ClassName: CommandCustomHeader 
* @Description: 
* @author LUCKY
* @date 2016年6月17日 下午3:33:36 
*  
*/
public interface CommandCustomHeader {

    public void checkFields() throws RemotingCommandException;
}
