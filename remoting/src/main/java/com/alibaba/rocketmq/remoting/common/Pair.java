/**
 * 
 */
package com.alibaba.rocketmq.remoting.common;

/** 
* @ClassName: Pair 
* @Description: 
* @author LUCKY
* @date 2016年6月17日 上午11:49:26 
*  
*/
public class Pair<T1, T2> {

    private T1 Object1;
    private T2 Object2;

    public Pair(T1 object1, T2 object2) {
        Object1 = object1;
        Object2 = object2;
    }

    public T1 getObject1() {
        return Object1;
    }

    public void setObject1(T1 object1) {
        Object1 = object1;
    }

    public T2 getObject2() {
        return Object2;
    }

    public void setObject2(T2 object2) {
        Object2 = object2;
    }

}
