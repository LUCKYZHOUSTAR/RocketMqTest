/**
 * 
 */
package com.alibaba.rocketmq.remoting.common;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.Enumeration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
* @ClassName: RemotingUtil 
* @Description: 
* @author LUCKY
* @date 2016年6月17日 下午1:06:12 
*  
*/
public class RemotingUtil {

    private static final Logger log              = LoggerFactory.getLogger("RocketmqRemoting");
    public static final String  OS_NAME          = System.getProperty("os.name");

    private static boolean      isLinuxPlatform  = false;
    private static boolean      isWindowPlatform = false;

    static {
        if ((OS_NAME != null) && (OS_NAME.toLowerCase().indexOf("linux") > 0)) {
            isLinuxPlatform = true;
        } else if ((OS_NAME != null) && (OS_NAME.toLowerCase().indexOf("windows") > 0)) {
            isWindowPlatform = true;
        }
    }

    public static boolean isLinuxPlatform() {
        return isLinuxPlatform;
    }

    public static boolean isWindowsPlatform() {
        return isWindowPlatform;
    }

    public static Selector openSelector() throws IOException {
        Selector result = null;

        if (isLinuxPlatform()) {
            try {
                Class providerClazz = Class.forName("sun.nio.ch.EPollSelectorProvider");
                if (providerClazz != null) {
                    try {
                        Method method = providerClazz.getMethod("provider", new Class[0]);
                        if (method != null) {
                            SelectorProvider selectorProvider = (SelectorProvider) method.invoke(
                                null, new Object[0]);
                            if (selectorProvider != null) {
                                result = selectorProvider.openSelector();
                            }
                        }
                    } catch (Exception e) {
                    }
                }
            } catch (Exception e) {
            }
        }

        if (result == null) {
            result = Selector.open();
        }

        return result;
    }

    public static String getLocalAddress() {
        try {
            Enumeration enumeration = NetworkInterface.getNetworkInterfaces();
            ArrayList<String> ipv4Result = new ArrayList();
            ArrayList<String> ipv6Result = new ArrayList();
            while (enumeration.hasMoreElements()) {
                NetworkInterface networkInterface = (NetworkInterface) enumeration.nextElement();
                Enumeration en = networkInterface.getInetAddresses();
                while (en.hasMoreElements()) {
                    InetAddress address = (InetAddress) en.nextElement();
                    if (!address.isLoopbackAddress()) {
                        if ((address instanceof Inet6Address)) {
                            ipv6Result.add(normalizeHostAddress(address));
                        } else {
                            ipv4Result.add(normalizeHostAddress(address));
                        }
                    }
                }

            }

            if (!ipv4Result.isEmpty()) {
                for (String ip : ipv4Result) {
                    if ((!ip.startsWith("127.0")) && (!ip.startsWith("192.168"))) {
                        return ip;
                    }
                }
                return (String) ipv4Result.get(ipv4Result.size() - 1);
            }
            if (!ipv6Result.isEmpty()) {
                return (String) ipv6Result.get(0);
            }

            InetAddress localHost = InetAddress.getLocalHost();
            return normalizeHostAddress(localHost);
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        return null;
    }

    /** 
    * @Title: normalizeHostAddress 
    * @Description:
    * @author LUCKY
    * @date 2016年6月17日 下午1:10:04 
    */
    public static String normalizeHostAddress(InetAddress localHost) {
        if (localHost instanceof Inet6Address) {
            return "[" + localHost.getHostAddress() + "]";
        }

        return localHost.getHostAddress();
    }

    public static SocketAddress string2SocketAddress(String addr) {
        String[] s = addr.split(":");
        InetSocketAddress isa = new InetSocketAddress(s[0], Integer.valueOf(s[1]).intValue());
        return isa;
    }

    public static String socketAddress2String(SocketAddress addr) {
        StringBuilder sb = new StringBuilder();
        InetSocketAddress inetSocketAddress = (InetSocketAddress) addr;
        sb.append(inetSocketAddress.getAddress().getAddress()).append(":")
            .append(inetSocketAddress.getPort());
        return sb.toString();
    }

    public static SocketChannel connect(SocketAddress remote) {
        return connect(remote, 5000);
    }

    public static SocketChannel connect(SocketAddress remote, int timeOutMillis) {

        SocketChannel sc = null;
        try {
            sc = SocketChannel.open();
            sc.configureBlocking(true);
            //默认的是操作系统时间，关闭连接后继续接受数据
            sc.socket().setSoLinger(false, -1);
            //没有延迟
            sc.socket().setTcpNoDelay(true);
            //默认为64M
            sc.socket().setReceiveBufferSize(65536);
            sc.socket().setSendBufferSize(65536);
            sc.socket().connect(remote, timeOutMillis);
            sc.configureBlocking(false);
            return sc;

        } catch (Exception e) {
            if (sc != null) {
                try {
                    sc.close();
                } catch (IOException e2) {
                    e2.printStackTrace();
                }
            }
        }

        return null;
    }

    public static void closeChannel(Channel channel) {
        final String addrRemote = RemotingHelper.parseChannelRemoteAddr(channel);
        channel.close().addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future) throws Exception {
                RemotingUtil.log.info(
                    "closeChannel: close the connection to remote address[{}] result: {}",
                    addrRemote, future.isSuccess());
            }
        });
    }

}
