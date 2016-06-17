/**
 * 
 */
package com.alibaba.rocketmq.remoting.protocol;

import java.nio.charset.Charset;

import com.alibaba.fastjson.JSON;

/** 
* @ClassName: RemotingSerializable 
* @Description: 
* @author LUCKY
* @date 2016年6月17日 下午3:15:18 
*  
*/
public class RemotingSerializable {

    public String toJson() {
        return toJson(false);
    }

    public String toJson(boolean prettyFormat) {
        return toJson(this, prettyFormat);
    }

    public static String toJson(Object obj, boolean prettyFormat) {
        return JSON.toJSONString(obj, prettyFormat);
    }

    public static <T> T fromJson(String json, Class<T> classOfT) {
        return JSON.parseObject(json, classOfT);
    }

    public byte[] encode() {
        String json = toJson();
        if (json != null) {
            return json.getBytes();
        }
        return null;
    }

    public static byte[] encode(Object obj) {
        String json = toJson(obj, false);
        if (json != null) {
            return json.getBytes(Charset.forName("UTF-8"));
        }
        return null;
    }

    public static <T> T decode(byte[] data, Class<T> classOfT) {
        String json = new String(data, Charset.forName("utf-8"));
        return fromJson(json, classOfT);
    }
}
