package com.alibaba.rocketmq.remoting.netty;

public class NettySystemConfig {
    public static final String SystemPropertyNettyPooledByteBufAllocatorEnable = "com.rocketmq.remoting.nettyPooledByteBufAllocatorEnable";
    public static boolean      NettyPooledByteBufAllocatorEnable               = Boolean
                                                                                   .parseBoolean(System
                                                                                       .getProperty(
                                                                                           "com.rocketmq.remoting.nettyPooledByteBufAllocatorEnable",
                                                                                           "false"));
    public static final String SystemPropertySocketSndbufSize                  = "com.rocketmq.remoting.socket.sndbuf.size";
    public static int          SocketSndbufSize                                = Integer
                                                                                   .parseInt(System
                                                                                       .getProperty(
                                                                                           "com.rocketmq.remoting.socket.sndbuf.size",
                                                                                           "65535"));
    public static final String SystemPropertySocketRcvbufSize                  = "com.rocketmq.remoting.socket.rcvbuf.size";
    public static int          SocketRcvbufSize                                = Integer
                                                                                   .parseInt(System
                                                                                       .getProperty(
                                                                                           "com.rocketmq.remoting.socket.rcvbuf.size",
                                                                                           "65535"));
    public static final String SystemPropertyClientAsyncSemaphoreValue         = "com.rocketmq.remoting.clientAsyncSemaphoreValue";
    public static int          ClientAsyncSemaphoreValue                       = Integer
                                                                                   .parseInt(System
                                                                                       .getProperty(
                                                                                           "com.rocketmq.remoting.clientAsyncSemaphoreValue",
                                                                                           "2048"));
    public static final String SystemPropertyClientOnewaySemaphoreValue        = "com.rocketmq.remoting.clientOnewaySemaphoreValue";
    public static int          ClientOnewaySemaphoreValue                      = Integer
                                                                                   .parseInt(System
                                                                                       .getProperty(
                                                                                           "com.rocketmq.remoting.clientOnewaySemaphoreValue",
                                                                                           "2048"));
}
