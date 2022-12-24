package nl.koop.overheid.movetocds.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {
  private static Properties props = loadProperties();
        
  public static String host = props.getProperty("marklogic.host");
  
  public static int port = Integer.parseInt(props.getProperty("marklogic.port"));
  
  public static String user = props.getProperty("marklogic.writer_user");
  
  public static String password = props.getProperty("marklogic.writer_password");
  
  public static String admin_user = props.getProperty("marklogic.admin_user");
  
  public static String admin_password = props.getProperty("marklogic.admin_password");
  
  protected static String authType = props.getProperty("marklogic.authentication_type").toUpperCase();

  protected static int batch_size = Integer.parseInt(props.getProperty("marklogic.batch_size"));

  protected static int thread_count = Integer.parseInt(props.getProperty("marklogic.thread_count"));

    // get the configuration for the marklogic CDS
  private static Properties loadProperties() {        
      try {
          String propsName = "Config.properties";
          InputStream propsStream =
              Config.class.getClassLoader().getResourceAsStream(propsName);
          if (propsStream == null)
              propsStream = Config.class.getResourceAsStream(propsName);
          if (propsStream == null)
              throw new IOException("Could not read config properties");

          Properties props = new Properties();
          props.load(propsStream);

          return props;

      } catch (final IOException exc) {
          throw new Error(exc);
      }  
  }
}
