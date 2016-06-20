package com.alibaba.rocketmq.remoting.netty;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

public class NettyDecoder extends LengthFieldBasedFrameDecoder {
    private static final Logger log              = LoggerFactory.getLogger("RocketmqRemoting");
    private static final int    FRAME_MAX_LENGTH = Integer.parseInt(System.getProperty(
                                                     "com.rocketmq.remoting.frameMaxLength",
                                                     "8388608"));

    public NettyDecoder() {
        super(FRAME_MAX_LENGTH, 0, 4, 0, 4);
    }

    @Override
    public Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {

        ByteBuf frame = null;
        try {
            frame = (ByteBuf) super.decode(ctx, in);
            if (null == frame) {
                return null;
            }
            ByteBuffer byteBuffer = frame.nioBuffer();

            return RemotingCommand.decode(byteBuffer);
        } catch (Exception e) {
            log.error("decode exception: " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            RemotingUtil.closeChannel(ctx.channel());
            if (null != frame) {
                frame.release();
            }
        } finally {
            if (null != frame) {
                frame.release();
            }
        }
        
        return null;
    }

}
