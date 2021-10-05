
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;
public class  ReadFromDB {
    String tablename;
    public ReadFromDB(String tablename){
        this.tablename=tablename;
    }
    private static final String url = "jdbc:mysql://localhost:3306/coviddb";
    public Dataset<Row> readFromMySql( SparkSession spark){
        ConnectToDb pr = new ConnectToDb();
        Properties properties = pr.connect();
        Dataset<Row> covid;
        covid=spark.read().jdbc(url,tablename,properties);

        return covid;
    }
}
