package com.datafibers.util;

/**
 * Constant class for all agents
 */
public class AgentConstant {

    //Server settings - need to move to config/param files later
    public static String SERVER_ADDR = "localhost";
    public static int SERVER_PORT = 8998;
    public static int RES_SUCCESS = 202;

    //Internal constant
    public static String FILE_NAME = null;
    public static String FILE_TOPIC = null;
    public static String META_TOPIC = "META_DATA"; //use default value for marking handshake
    public static String TRANS_MODE = null;

    //File processing folder setting - not used right now since we pick up directly from file folder parsed from CML
    public static String PROCESSING_FOLDER = "/home/vagrant/processing/";
    public static String PROCESSED_FOLDER = "/home/vagrant/processed/";
    public static String LANDING_FOLDER = "/home/vagrant/landing/";

    //File extensions internal used or ignored
    public static String IGNORE_FILES_POSTFIX = ".ignore";
    public static String PROCESSING_FILES_POSTFIX = ".processing";
    public static String PROCESSED_FILES_POSTFIX = ".processed";
    public static String FILE_FILTER_REGX = ".\\.(?!(js|exe" +
            (IGNORE_FILES_POSTFIX + PROCESSING_FILES_POSTFIX + PROCESSED_FILES_POSTFIX).replace('.','|') +
            ")$)([^.]+$)";

    //Vertx and other control parameters
    public static int COUNTER = 0;
    public static int FILE_WATCHER_PERIODIC = 5000; //1000 is 1 second


}
