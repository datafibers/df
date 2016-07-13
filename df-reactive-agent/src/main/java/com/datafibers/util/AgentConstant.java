package com.datafibers.util;

/**
 * Constant class for all agents
 */
public class AgentConstant {

    public static String FILE_NAME = null;
    public static String FILE_TOPIC = null;
    public static String META_TOPIC = "META_DATA"; //use default value for marking handshake
    public static String TRANS_MODE = null;
    public static String SERVER_ADDR = "localhost";
    public static int SERVER_PORT = 8998;
    public static int RES_SUCCESS = 202;
    public static String IGNORE_FILES_PREFIX = "_";
    public static String PROCESSING_FILES_POSTFIX = ".processing";
    public static String PROCESSED_FILES_POSTFIX = ".processed";
    public static int COUNTER = 0;
    public static String PROCESSING_FOLDER = "/home/vagrant/processing/";
    public static String PROCESSED_FOLDER = "/home/vagrant/processed/";
    public static String LANDING_FOLDER = "/home/vagrant/landing/";
    public static String FILE_FILTER_REGX = "([^\\s]+(\\.(?i)(txt|csv|json|xml))$)";
    public static int FILE_WATCHER_PERIODIC = 1000; //1000 is 1 second
}
