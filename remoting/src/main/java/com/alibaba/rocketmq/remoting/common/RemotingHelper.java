/**
 * 
 */
package com.alibaba.rocketmq.remoting.common;

import io.netty.channel.Channel;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

/** 
* @ClassName: RemotingHelper 
* @Description: 
* @author LUCKY
* @date 2016年6月17日 下午1:48:00 
*  
*/
public class RemotingHelper {
    public static final String RemotingLogName = "RocketmqRemoting";

    public static String exceptionSimpleDesc(Throwable e) {
        StringBuffer sb = new StringBuffer();
        if (e != null) {
            sb.append(e.toString());
        }
        StackTraceElement[] stackTrace = e.getStackTrace();
        if ((stackTrace != null) && (stackTrace.length > 0)) {
            StackTraceElement element = stackTrace[0];
            sb.append(", ").append(element.toString());
        }

        return sb.toString();
    }

    public static SocketAddress string2SocketAddress(String addr) {
        String[] a = addr.split(":");
        InetSocketAddress isa = new InetSocketAddress(a[0], Integer.valueOf(a[1]).intValue());
        return isa;
    }


    public static String parseChannelRemoteAddr(Channel channel) {
        if (null == channel) {
            return "";
        }
        SocketAddress remote = channel.remoteAddress();
        String addr = remote != null ? remote.toString() : "";
        if (addr.length() > 0) {
            int index = addr.indexOf("/");
            if (index >= 0) {
                return addr.substring(index + 1);
            }
            return addr;
        }

        return "";
    }

    public static String parseChannelRemoteName(Channel channel) {
        if (null == channel) {
            return "";
        }
        InetSocketAddress remote = (InetSocketAddress) channel.remoteAddress();
        if (remote != null) {
            return remote.getAddress().getHostName();
        }
        return "";
    }

    public static String parseSocketAddressAddr(SocketAddress socketAddress) {

        if (socketAddress != null) {
            String addr = socketAddress.toString();
            if (addr.length() > 0) {
                return addr.substring(1);
            }
        }
        return "";

    }

    public static String parseSocketAddressName(SocketAddress socketAddress) {
        InetSocketAddress address = (InetSocketAddress) socketAddress;
        if (address != null) {
            return address.getAddress().getHostName();
        }

        return "";
    }
}
