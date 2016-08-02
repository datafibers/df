package com.datafibers.util;

import com.datafibers.conf.ConstantApp;

import io.vertx.core.MultiMap;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The message filter is used to 
 * 1) filter message received from client using java regular expression;
 * 2) transform the data based on the input parameter
 */
public class MsgTrans {

    public static Pattern getPattern (String filter, String inputString) {

        switch (filter == null ? "" : filter.toUpperCase()) {
            case "JSON":
                return Pattern.compile(Pattern.quote(ConstantApp.JSON_PATTERN_L) + "(.*?)" + Pattern.quote(ConstantApp.JSON_PATTERN_R));
            case "JSON_KAFKA":  
            	
            default:
                return Pattern.compile(".+");
        }
    }
    
    public static String dataTrans(String transCode, String inputString) {
    	switch (transCode) {
	    	case "JRTKC":
	    	    inputString = JSONReformatToKAFKAConnect(inputString);
	    	    break;
	    	default:
    		
    	}
    	
    	return inputString;
    }
    
    /**
     * Convert JSON data input format to 
     * 
     * @param sCurrentLine
     * @return
     */
    protected static String JSONReformatToKAFKAConnect(String sCurrentLine) {
		String REGEX0 = "/";
		String REGEX1 = "\"";
		
		String REPLACE0 = "\\\\";
		String REPLACE1 = "\\\\\"";
		
		String output0 = null;
		String output1 = null;
		String finalOutput= null;
		Pattern pattern0, pattern1 = null;
		Matcher matcher0, matcher1 = null;
		
		System.out.println(sCurrentLine);
		
		pattern0 = Pattern.compile(REGEX0);
		matcher0 = pattern0.matcher(sCurrentLine);
		output0 = matcher0.replaceAll(REPLACE0);
		
		pattern1 = Pattern.compile(REGEX1);
		matcher1 = pattern1.matcher(sCurrentLine);
		output1 = matcher1.replaceAll(REPLACE1);
		
		finalOutput = "\"" + output1 + "\"";
		ServerFunc.printToConsole("INFO", "Output after data transfor is: " + finalOutput);
		
		return finalOutput;
	}
}
