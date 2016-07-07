package com.datafibers.util;

import com.datafibers.conf.ConfigApp;
import io.vertx.core.MultiMap;

import java.util.Map;

/**
 * Class for commonly used server functions
 */
public class ServerFunc {

    public static void printToConsole(String msg_type, MultiMap mp, Boolean debug) {
        if(debug) {
            for (Map.Entry entry : mp.entries()) {
                System.out.println( msg_type + ": The MultiMap KEY:VALUE pairs for HEADER is: [" + entry.getKey() + ":" +
                        entry.getValue() +"]");
            }
        }
    }

    public static void printToConsole(String msg_type, MultiMap mp) {
        if(ConfigApp.getServerDebugMode()) {
            for (Map.Entry entry : mp.entries()) {
                System.out.println( msg_type + ": The MultiMap KEY:VALUE pairs for HEADER is: [" + entry.getKey() + ":" +
                        entry.getValue() +"]");
            }
        }
    }

    public static void printToConsole(String msg_type, String msg,  Boolean debug) {
        if(debug) {
            System.out.println(msg_type + ": " + msg);
        }
    }

    public static void printToConsole(String msg_type, String msg) {
        if(ConfigApp.getServerDebugMode()) {
            System.out.println(msg_type + ": " + msg);
        }
    }
}
