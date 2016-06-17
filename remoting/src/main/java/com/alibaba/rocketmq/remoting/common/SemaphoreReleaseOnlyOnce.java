/**
 * 
 */
package com.alibaba.rocketmq.remoting.common;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/** 
* @ClassName: SemaphoreReleaseOnlyOnce 
* @Description: 
* @author LUCKY
* @date 2016年6月17日 下午2:05:58 
*  
*/
public class SemaphoreReleaseOnlyOnce {

    private final AtomicBoolean released = new AtomicBoolean(false);
    private final Semaphore     semaphore;

    public SemaphoreReleaseOnlyOnce(Semaphore semaphore) {
        this.semaphore = semaphore;
    }

    public void release() {
        if ((this.semaphore != null) && (this.released.compareAndSet(false, true))) {
            this.semaphore.release();
        }
    }

    public Semaphore getSemaphore() {
        return this.semaphore;
    }
}
