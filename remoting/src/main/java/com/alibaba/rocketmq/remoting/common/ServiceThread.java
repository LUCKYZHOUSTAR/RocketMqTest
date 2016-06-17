/**
 * 
 */
package com.alibaba.rocketmq.remoting.common;

import org.omg.CORBA.PUBLIC_MEMBER;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
* @ClassName: ServiceThread 
* @Description: 
* @author LUCKY
* @date 2016年6月17日 下午2:33:47 
*  
*/
public abstract class ServiceThread implements Runnable {

    private static final Logger stlog       = LoggerFactory.getLogger("RocketmqRemoting");
    protected final Thread      thread;
    private static final long   JoinTime    = 90000L;
    //线程处于是否唤醒状态
    protected volatile boolean  hasNotified = false;
    protected volatile boolean  stoped      = false;

    public ServiceThread() {
        this.thread = new Thread(this, getServiceName());
    }

    public abstract String getServiceName();

    public void start() {
        this.thread.start();
    }

    public void shutdown() {
        shutDown(false);
    }

    public void stop() {
        stop(false);
    }

    public void makeStop() {
        this.stoped = true;
        stlog.info("makestop thread" + getServiceName());
    }

    public void stop(boolean interrupt) {
        this.stoped = true;
        stlog.info("stop thread " + getServiceName() + " interrupt " + interrupt);
        synchronized (this) {
            if (!this.hasNotified) {
                this.hasNotified = true;
                notify();
            }

            if (interrupt) {
                this.thread.interrupt();
            }
        }

    }

    public void shutDown(boolean interrupt) {
        this.stoped = true;
        stlog.info("shutdown thread " + getServiceName() + " interrupt " + interrupt);
        synchronized (this) {
            if (!this.hasNotified) {
                this.hasNotified = true;
                notify();
            }

            try {
                if (interrupt) {
                    this.thread.interrupt();
                }
                long beginTime = System.currentTimeMillis();
                this.thread.join(getJointTime());
                long elicpseTime = System.currentTimeMillis() - beginTime;
                stlog.info("join thread " + getServiceName() + "elicpse time(ms) " + elicpseTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void wakeUp() {
        synchronized (this) {
            if (!this.hasNotified) {
                this.hasNotified = true;
                notify();
            }
        }
    }

    protected void waitForRunning(long interval) {
        synchronized (this) {
            //true：唤醒   false：wait状态
            if (this.hasNotified) {
                this.hasNotified = false;
                onWaitEnd();
                return;
            }
            try {
                wait(interval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                this.hasNotified = false;
                onWaitEnd();
            }
        }
    }

    protected void onWaitEnd() {

    }

    public boolean isStoped() {
        return this.stoped;
    }

    public long getJointTime() {
        return 900000L;
    }
}
