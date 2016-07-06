package io.example.conf;

import io.example.vertx.util.ServerFunc;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.Iterator;
import java.util.Map;

/**
 * This class encapsulates access to Hadoop Environment configuration.
 */
public class ConfigHadoop {

  private static Configuration hadoopConf;

  /*
   * Hadoop configurations. Hadoop cluster info get from ConfigApp.
   * Once Hadoop is available, get other information from ConfigHadoop
   */
  private static final String CORE_SITE_XML_PATH_KEY = "hadoop.core.site.path";
  private static final String HDFS_SITE_XML_PATH_KEY = "hadoop.hdfs.site.path";
  private static final String YARN_SITE_XML_PATH_KEY = "hadoop.yarn.site.path";

  private static final String HDFS_SHARED_CONF_KEY = "hadoop.hdfs.shared.conf";
  private static final String HDFS_KEYTAB_FILE_KEY = "hadoop.hdfs.keytab.file";
  private static final String HDFS_KEYTAB_PRINCIPAL_KEY = "hadoop.hdfs.keytab.principal";
  private static final String HDFS_KEYTAB_HOSTNAME_KEY = "hadoop.hdfs.keytab.hostname";

  private static final String NAMENODE_ADDRESS_KEY = "hadoop.dfs.nameservices";
  private static final String YARN_RESOURCE_MANAGER_ADDRESS_KEY = "hadoop.yarn.resourcemanager.cluster-id";



  public static String getCoreSiteXmlPath() {
    return ConfigApp.getConfigurationParameterValue(ConfigApp.getAppConfig(), CORE_SITE_XML_PATH_KEY);
  }

  public static String getHdfsSiteXmlPath() {
    return ConfigApp.getConfigurationParameterValue(ConfigApp.getAppConfig(), HDFS_SITE_XML_PATH_KEY);
  }

  public static String getYarnSiteXmlPath() {
    return ConfigApp.getConfigurationParameterValue(ConfigApp.getAppConfig(), YARN_SITE_XML_PATH_KEY);
  }

  public static String getHdfsKeyTabFileKey() {
    return HDFS_KEYTAB_FILE_KEY;
  }

  public static String getHdfsKeyTabFile() {
    return ConfigApp.getConfigurationParameterValue(ConfigApp.getAppConfig(), getHdfsKeyTabFileKey());
  }

  public static String getHdfsKeyTabPrincipalKey() {
    return HDFS_KEYTAB_PRINCIPAL_KEY;
  }

  public static String getHdfsKeyTabPrincipal() {
    return ConfigApp.getConfigurationParameterValue(ConfigApp.getAppConfig(), getHdfsKeyTabPrincipalKey());
  }

  public static String getHdfsKeyTabHostname() {
    return ConfigApp.getConfigurationParameterValue(ConfigApp.getAppConfig(), HDFS_KEYTAB_HOSTNAME_KEY);
  }

  public static String getNamenodeAddressKey() {
    return NAMENODE_ADDRESS_KEY;
  }

  /**
   * This will return address of the name node in format hdfs://namenodeaddress
   */
  public static String getNamenodeHdfsAddress() {
    return "hdfs://" + getHadoopConfigurationParameterValue(getHadoopConfig(), NAMENODE_ADDRESS_KEY);
  }

  public static String getLocalNamenodeHdfsValue() {
    return ConfigApp.getConfigurationParameterValue(ConfigApp.getAppConfig(), NAMENODE_ADDRESS_KEY);
  }

  public static String getYarnResourceManagerAddressKey() {
    return YARN_RESOURCE_MANAGER_ADDRESS_KEY;
  }

  public static String getResourceManagerAddress() {
    return getHadoopConfigurationParameterValue(getHadoopConfig(), YARN_RESOURCE_MANAGER_ADDRESS_KEY);
  }

  public static String getLocalResourceManagerAddress() {
    return ConfigApp.getConfigurationParameterValue(ConfigApp.getAppConfig(), YARN_RESOURCE_MANAGER_ADDRESS_KEY);
  }

  public static Configuration getHadoopConfig() {
    if (hadoopConf == null) {
      hadoopConf = new Configuration();

      hadoopConf.addResource(new Path(getCoreSiteXmlPath()));
      hadoopConf.addResource(new Path(getHdfsSiteXmlPath()));
      hadoopConf.addResource(new Path(getYarnSiteXmlPath()));

      // This is needed for correct class resolution at run time
      hadoopConf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
      hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

      // This value will get set above in the getYarnSiteXml. For local machines, get the app conf
      // value:
      if (StringUtils.isEmpty(hadoopConf.get(getYarnResourceManagerAddressKey()))) {
        hadoopConf.set(getYarnResourceManagerAddressKey(), getLocalResourceManagerAddress());
        ServerFunc.printToConsole("WARN","Running local yarn resource manager.  This is only for non-clustered environment!");
      }
      // This value will get set above in the getHdfsSiteXml. For local machines, get the app conf
      // value:
      if (StringUtils.isEmpty(hadoopConf.get(getNamenodeAddressKey()))) {
        hadoopConf.set(getNamenodeAddressKey(), getLocalNamenodeHdfsValue());
        ServerFunc.printToConsole("WARN","Running local name node manager.  This is only for non-clustered environment!");
      }

      // Security:
      hadoopConf.set(getHdfsKeyTabFileKey(), getHdfsKeyTabFile());
      hadoopConf.set(getHdfsKeyTabPrincipalKey(),getHdfsKeyTabPrincipal());

      Iterator<Map.Entry<String, String>> iterator = hadoopConf.iterator();
      while (iterator.hasNext()) {
        Map.Entry<String, String> next = iterator.next();
        ServerFunc.printToConsole("INFO","HADOOP_CONF:" + next.getKey() + ":" + next.getValue());
      }
    }

    return hadoopConf;
  }

  public static String getHadoopConfigurationParameterValue(Configuration conf, String parameterKey) {
    String value = conf.get(parameterKey);
    if (StringUtils.isEmpty(value))
      throw new RuntimeException("Unable to determine value for " + parameterKey + ", found :" + value);
    return value;
  }

}
