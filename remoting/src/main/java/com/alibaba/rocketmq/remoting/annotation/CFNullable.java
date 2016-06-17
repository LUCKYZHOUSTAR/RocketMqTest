/**
 * 
 */
package com.alibaba.rocketmq.remoting.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Retention;
import java.lang.annotation.Documented;
import java.lang.annotation.Target;

/** 
* @ClassName: CFNullAble 
* @Description: 表示字段可以为空
* @author LUCKY
* @date 2016年6月17日 上午11:46:52 
*  
*/

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD,ElementType.LOCAL_VARIABLE,ElementType.METHOD,ElementType.PARAMETER})
public @interface CFNullable {

}
