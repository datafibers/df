package io.example.conf;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;

/**
 * This class encapsulates access to DF Server Configuration.
 */
public class ConfigApp {

  private static Configuration appConf;

  // App configuration
  private static final String SERVER_PORT = "df.server.port";
  private static final String SERVER_TMP = "df.server.tmp.folder";
  private static final String DEBUG_MODE = "df.server.debug.mode";


  public static Configuration getAppConfig() {
    try {
      // Return or initialize on first access
      if (appConf == null)
        appConf = new PropertiesConfiguration(ConstantApp.APP_PROPERTIES_FILE);

      return appConf;
    } catch (ConfigurationException e) {
      // nothing we can do about this, just re-throw
      throw new RuntimeException(e);
    }
  }

  public static String getConfigurationParameterValue(Configuration conf, String parameterKey) {
    String value = conf.getString(parameterKey);
    if (StringUtils.isEmpty(value))
      throw new RuntimeException("Unable to determine value for " + parameterKey + ", found :" + value);
    return value;
  }

  public static Integer getServerPort() {
    return Integer.parseInt(getConfigurationParameterValue(getAppConfig(), SERVER_PORT));
  }

  public static String getServerTmp() {
    return getConfigurationParameterValue(getAppConfig(), SERVER_TMP);
  }

  public static Boolean getServerDebugMode() {
    return Boolean.valueOf(getConfigurationParameterValue(getAppConfig(), DEBUG_MODE));
  }

}
