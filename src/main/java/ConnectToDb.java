import java.util.Properties;

public class ConnectToDb {
    private static final String password="predator(9922)";

public Properties connect() {
    Properties properties = new Properties();
    properties.setProperty("driver", "com.mysql.jdbc.Driver");
    properties.setProperty("user", "root");
    properties.setProperty("password", password);
    return properties;
}
}
