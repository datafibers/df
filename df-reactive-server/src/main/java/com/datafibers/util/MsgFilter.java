package com.datafibers.util;

import com.datafibers.conf.ConstantApp;

import java.util.regex.Pattern;

/**
 * The message filter is used to filter message received from client using java regular expression
 */
public class MsgFilter {

    public static Pattern getPattern (String filter) {

        switch (filter == null?"":filter.toUpperCase()) {
            case "JSON":
                return Pattern.compile(Pattern.quote(ConstantApp.JSON_PATTERN_L) + "(.*?)" + Pattern.quote(ConstantApp.JSON_PATTERN_R));
            default:
                return Pattern.compile(".+");
        }
    }
}
