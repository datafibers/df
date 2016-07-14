package com.datafibers.conf;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;

/**
 * This class encapsulates access to DF Server Configuration.
 */
public class ConfigApp {

  private static Configuration appConf;

  // App configuration
  private static final String STOCK_LIST = "demo.stock.list";
  private static final String STAGE_DIR = "demo.stock.stage.dir";
  private static final String FILE_WATCHER_PERIODIC = "demo.stock.run.periodic";

  public static Configuration getAppConfig(String propFile) {
    try {
      // Return or initialize on first access
      if (appConf == null)
        appConf = new PropertiesConfiguration(propFile);

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

  public static String[] getStockList() {
    return getConfigurationParameterValue(appConf, STOCK_LIST).split(";");
  }

  public static String getStockStageDir() {
    return getConfigurationParameterValue(appConf, STAGE_DIR);
  }

  public static int getPeriodic() {
    return Integer.parseInt(getConfigurationParameterValue(appConf, FILE_WATCHER_PERIODIC));
  }

}
