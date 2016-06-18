/**
 * 
 */
package com.alibaba.rocketmq.remoting.protocol;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;

/** 
* @ClassName: RemotingCommand 
* @Description: Remoting模块中，服务器与客户端通过传递RemotingCommand来交互
* @author LUCKY
* @date 2016年6月17日 下午3:18:44 
*  
*/
public class RemotingCommand {

    public static String                  RemotingVersionKey = "rocketmq.remoting.version";
    private static volatile int           ConfigVersion      = -1;
    private static AtomicInteger          RequestId          = new AtomicInteger(0);
    private static final int              RPC_TYPE           = 0;
    private static final int              PRC_ONEWAY         = 1;
    private int                           code;
    private LanguageCode                  languageCode       = LanguageCode.JAVA;
    private int                           version            = 0;
    private int                           opaque             = RequestId.getAndIncrement();
    private int                           flag               = 0;
    private String                        remark;
    private HashMap<String, String>       extFileds;
    private transient CommandCustomHeader customHeader;
    private transient byte[]              body;
    private static final String           StringName         = String.class.getCanonicalName();
    private static final String           IntegerName1       = Integer.class.getCanonicalName();
    private static final String           IntegerName2       = Integer.TYPE.getCanonicalName();
    private static final String           LongName1          = long.class.getCanonicalName();
    private static final String           LongName2          = Long.TYPE.getCanonicalName();

    private static final String           BooleanName1       = Boolean.class.getCanonicalName();
    private static final String           BooleanName2       = Boolean.TYPE.getCanonicalName();
    private static final String           DoubleName1        = Double.class.getCanonicalName();
    private static final String           DoubleName2        = Double.TYPE.getCanonicalName();

    public static RemotingCommand createRequestCommand(int code, CommandCustomHeader customHeader) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.customHeader = customHeader;
        setCmdVersion(cmd);
        return cmd;
    }

    public static RemotingCommand createResponseCommand(Class<? extends CommandCustomHeader> classHeader) {
        RemotingCommand cmd = createResponseCommand(1, "not set any response code", classHeader);
        return cmd;
    }

    public static RemotingCommand createResponseCommand(int code, String remark) {
        return createResponseCommand(code, remark, null);
    }

    public static RemotingCommand createResponseCommand(int code,
                                                        String remark,
                                                        Class<? extends CommandCustomHeader> classHeader) {

        RemotingCommand cmd = new RemotingCommand();
        cmd.markResponseType();
        cmd.setCode(code);
        cmd.setRemark(remark);
        setCmdVersion(cmd);
        if (classHeader != null) {
            try {
                CommandCustomHeader objectHeader = (CommandCustomHeader) classHeader.newInstance();
                cmd.customHeader = objectHeader;
            } catch (InstantiationException e) {
                return null;
            } catch (IllegalAccessException e) {
                return null;
            }
        }

        return cmd;
    }

    private static void setCmdVersion(RemotingCommand cmd) {
        if (ConfigVersion >= 0) {
            cmd.setVersion(ConfigVersion);
        } else {
            String v = System.getProperty(RemotingVersionKey);
            if (v != null) {
                int value = Integer.parseInt(v);
                cmd.setVersion(value);
                ConfigVersion = value;
            }
        }
    }

    public CommandCustomHeader readCustomHeader() {
        return this.customHeader;
    }

    public void writeCustomHeader(CommandCustomHeader customHeader) {
        this.customHeader = customHeader;
    }

    public CommandCustomHeader decodeCommandCustomHeader(Class<? extends CommandCustomHeader> classHeader)
                                                                                                          throws RemotingCommandException {
        if (this.extFileds != null) {
            CommandCustomHeader objectHeader;
            try {
                objectHeader = (CommandCustomHeader) classHeader.newInstance();
            } catch (InstantiationException e) {
                return null;
            } catch (IllegalAccessException e) {
                return null;
            }

            Field[] fields = objectHeader.getClass().getDeclaredFields();
            for (Field field : fields) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    String fieldName = field.getName();
                    if (!fieldName.startsWith("this")) {
                        try {
                            String value = (String) this.extFileds.get(fieldName);
                            if (null == value) {
                                Annotation annotation = field.getAnnotation(CFNotNull.class);
                                if (annotation != null) {
                                    throw new RemotingCommandException("the custom field <"
                                                                       + fieldName + "> is null");
                                }

                            } else {
                                field.setAccessible(true);
                                String type = field.getType().getCanonicalName();
                                Object valueParsed = null;

                                if (type.equals(StringName)) {
                                    valueParsed = value;
                                } else if ((type.equals(IntegerName1))
                                           || (type.equals(IntegerName2))) {
                                    valueParsed = Integer.valueOf(Integer.parseInt(value));
                                } else if ((type.equals(LongName1)) || (type.equals(LongName2))) {
                                    valueParsed = Long.valueOf(Long.parseLong(value));
                                } else if ((type.equals(BooleanName1))
                                           || (type.equals(BooleanName2))) {
                                    valueParsed = Boolean.valueOf(Boolean.parseBoolean(value));
                                } else if ((type.equals(DoubleName1)) || (type.equals(DoubleName2))) {
                                    valueParsed = Double.valueOf(Double.parseDouble(value));
                                } else {
                                    throw new RemotingCommandException("the custom field <"
                                                                       + fieldName
                                                                       + "> type is not supported");
                                }

                                field.set(objectHeader, valueParsed);
                            }
                        } catch (Throwable e) {
                        }
                    }
                }
            }
            objectHeader.checkFields();

            return objectHeader;
        }

        return null;
    }

    //扩展EXT字段
    public void makeCustomHeaderToNet() {
        if (this.customHeader != null) {

            Field[] fields = this.customHeader.getClass().getDeclaredFields();
            if (null == this.extFileds) {
                this.extFileds = new HashMap<String, String>();
            }

            for (Field field : fields) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    String name = field.getName();
                    if (!name.startsWith("this")) {
                        Object valObject = null;
                        try {
                            field.setAccessible(true);
                            valObject = field.get(this.customHeader);
                        } catch (IllegalAccessException e) {
                        }

                        if (valObject != null) {
                            this.extFileds.put(name, valObject.toString());
                        }
                    }
                }
            }
        }
    }

    public ByteBuffer encodeHeader() {
        return encodeHeader(this.body != null ? this.body.length : 0);
    }

    //TODO 这段代码有问题
    public ByteBuffer encodeHeader(int bodyLength) {
        int length = 4;
        byte[] headerData = buildHeader();
        length += headerData.length;
        length += bodyLength;
        //        length=4+bodylength+headerData+4
        ByteBuffer result = ByteBuffer.allocate(4 + length - bodyLength);
        result.putInt(length);
        result.putInt(headerData.length);
        result.put(headerData);
        result.flip();
        return result;
    }

    private byte[] buildHeader() {
        makeCustomHeaderToNet();
        return RemotingSerializable.encode(this);
    }

    public static RemotingCommand decode(byte[] byteArray) {
        ByteBuffer buffer = ByteBuffer.wrap(byteArray);
        return decode(buffer);
    }

    public ByteBuffer encode() {
        int length = 4;
        byte[] headerData = buildHeader();
        length += headerData.length;
        if (this.body != null) {
            length += this.body.length;
        }

        ByteBuffer result = ByteBuffer.allocate(4 + length);
        result.putInt(length);
        result.putInt(headerData.length);
        result.put(headerData);
        if (this.body != null) {
            result.put(this.body);
        }

        result.flip();
        return result;
    }

    public static RemotingCommand decode(ByteBuffer byteBuffer) {
        int length = byteBuffer.limit();
        //length=4+headerdatalength+bodylength
        int headerLength = byteBuffer.getInt();
        byte[] headerBody = new byte[headerLength];
        byteBuffer.get(headerBody);
        int bodyLength = length - 4 - headerLength;
        byte[] bodyData = null;
        if (bodyLength > 0) {
            bodyData = new byte[bodyLength];
            byteBuffer.get(bodyData);
        }
        RemotingCommand cmd = RemotingSerializable.decode(headerBody, RemotingCommand.class);
        cmd.body = bodyData;
        return cmd;
    }

    public void markResponseType() {
        int bits = 1;
        this.flag |= bits;
    }

    public void markOnewayRPC() {
        int bits = 2;
        this.flag |= bits;
    }

    @JSONField(serialize = false)
    public boolean isOnewayRPC() {
        int bits = 2;
        return (this.flag & bits) == bits;
    }

    public int getCode() {
        return this.code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    @JSONField(serialize = false)
    public boolean isResponseType() {
        int bits = 1;
        return (this.flag & bits) == bits;
    }

    @JSONField(serialize = false)
    public RemotingCommandType getType() {
        if (isResponseType()) {
            return RemotingCommandType.RESPONSE_COMMAND;
        }
        return RemotingCommandType.REQUEST_COMMAND;

    }

    public LanguageCode getLanguage() {
        return this.languageCode;
    }

    public void setLanguage(LanguageCode language) {
        this.languageCode = language;
    }

    public int getVersion() {
        return this.version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getOpaque() {
        return this.opaque;
    }

    public void setOpaque(int opaque) {
        this.opaque = opaque;
    }

    public int getFlag() {
        return this.flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public String getRemark() {
        return this.remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public byte[] getBody() {
        return this.body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public HashMap<String, String> getExtFields() {
        return this.extFileds;
    }

    public void setExtFields(HashMap<String, String> extFields) {
        this.extFileds = extFields;
    }

    public static int createNewRequestId() {
        return RequestId.incrementAndGet();
    }

    public void addExtField(String key, String value) {
        if (null == this.extFileds) {
            this.extFileds = new HashMap<String, String>();
        }
        this.extFileds.put(key, value);
    }

    public String toString() {
        return "RemotingCommand [code=" + this.code + ", language=" + this.languageCode
               + ", version=" + this.version + ", opaque=" + this.opaque + ", flag(B)="
               + Integer.toBinaryString(this.flag) + ", remark=" + this.remark + ", extFields="
               + this.extFileds + "]";
    }
}
