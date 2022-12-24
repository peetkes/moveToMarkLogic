package nl.koop.overheid.movetocds.utils;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;

public class MarkLogicUtility {
  
  public static DatabaseClient getDatabaseClient(String username, String password) {
    DatabaseClientFactory.SecurityContext auth = Config.authType.equalsIgnoreCase("basic")
        ? new DatabaseClientFactory.BasicAuthContext(username, password)
        : new DatabaseClientFactory.DigestAuthContext(username, password);
        
    DatabaseClient client = DatabaseClientFactory.newClient(Config.host, Config.port, auth);
    return client;
  }
}
