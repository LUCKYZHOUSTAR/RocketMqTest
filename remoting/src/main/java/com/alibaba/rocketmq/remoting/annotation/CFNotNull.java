/**
 * 
 */
package com.alibaba.rocketmq.remoting.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** 
* @ClassName: CFNotNull 
* @Description: 表示字段不允许为空
* @author LUCKY
* @date 2016年6月17日 上午11:44:40 
*  
*/
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER, ElementType.LOCAL_VARIABLE })
public @interface CFNotNull {

}
