package com.datafibers.conf;

public final class ConstantApp {

    public static final String APP_PROPERTIES_FILE = "app.properties";

    public static final Boolean DEBUG_MODE = true;
    public static final String META_TOPIC = "metadata";

    public static final String DF_PROTOCOL_REGISTER = "REGISTER";
    public static final String DF_PROTOCOL_UNREGISTER = "UNREGISTER";
    public static final String DF_PROTOCOL_ACTIONS = "ACTIONS";

    public static final String DF_MODE_STREAM_KAFKA = "STREAM_KAFKA";
    public static final String DF_MODE_STREAM_HDFS = "STREAM_HDFS";
    public static final String DF_MODE_BATCH_HDFS = "BATCH_HDFS";
    public static final String DF_MODE_BATCH_HIVE = "BATCH_HIVE";
    public static final String DF_MODE_KAFKA_CONNECT_JSON = "KAFKA_CONNECT_JSON";

    public static final String DF_TYPE_MEATA = "META";
    public static final String DF_TYPE_PAYLOAD = "PAYLOAD";

    public static final String JSON_PATTERN_L = "{";
    public static final String JSON_PATTERN_R = "}";

    public enum MessageFilter {NONE, JSON}

}
