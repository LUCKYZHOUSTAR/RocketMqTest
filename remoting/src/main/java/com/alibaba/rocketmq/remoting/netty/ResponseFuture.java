package com.alibaba.rocketmq.remoting.netty;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.alibaba.rocketmq.remoting.InvokeCallback;
import com.alibaba.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

public class ResponseFuture {

    private volatile RemotingCommand       responseCommand;
    private volatile boolean               sendRequestOK           = true;
    private volatile Throwable             cause;
    private final int                      opaque;
    private final long                     timeOutMillis;
    private final InvokeCallback           invokeCallback;
    private final long                     beginTimeStamp          = System.currentTimeMillis();
    private final CountDownLatch           countDownLatch          = new CountDownLatch(1);
    private final SemaphoreReleaseOnlyOnce once;
    private final AtomicBoolean            executeCallBackOnlyOnce = new AtomicBoolean(false);

    public ResponseFuture(int opaque, long timeoutMillis, InvokeCallback invokeCallback,
                          SemaphoreReleaseOnlyOnce once) {
        this.opaque = opaque;
        this.timeOutMillis = timeoutMillis;
        this.invokeCallback = invokeCallback;
        this.once = once;
    }

    public void executeInvokeCallback() {
        if ((this.invokeCallback != null)
            && (this.executeCallBackOnlyOnce.compareAndSet(false, true))) {
            this.invokeCallback.operationComplete(this);
        }
    }

    public void release() {
        if (this.once != null) {
            this.once.release();
        }
    }

    public boolean isTimeOut() {
        long diff = System.currentTimeMillis() - this.beginTimeStamp;
        return diff > timeOutMillis;
    }

    public RemotingCommand waitResponse(long timeOutMillis) throws InterruptedException {
        this.countDownLatch.await(timeOutMillis, TimeUnit.MILLISECONDS);
        return this.responseCommand;
    }

    public void putResponse(RemotingCommand responseCommand) {
        this.responseCommand = responseCommand;
        this.countDownLatch.countDown();
    }

    public long getBeginTimestamp() {
        return this.beginTimeStamp;
    }

    public boolean isSendRequestOK() {
        return this.sendRequestOK;
    }

    public void setSendRequestOK(boolean sendRequestOK) {
        this.sendRequestOK = sendRequestOK;
    }

    public long getTimeoutMillis() {
        return this.timeOutMillis;
    }

    public InvokeCallback getInvokeCallback() {
        return this.invokeCallback;
    }

    public Throwable getCause() {
        return this.cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }

    public RemotingCommand getResponseCommand() {
        return this.responseCommand;
    }

    public void setResponseCommand(RemotingCommand responseCommand) {
        this.responseCommand = responseCommand;
    }

    public int getOpaque() {
        return this.opaque;
    }

    public String toString() {
        return "ResponseFuture [responseCommand=" + this.responseCommand + ", sendRequestOK="
               + this.sendRequestOK + ", cause=" + this.cause + ", opaque=" + this.opaque
               + ", timeoutMillis=" + this.timeOutMillis + ", invokeCallback="
               + this.invokeCallback + ", beginTimestamp=" + this.beginTimeStamp
               + ", countDownLatch=" + this.countDownLatch + "]";
    }

}
