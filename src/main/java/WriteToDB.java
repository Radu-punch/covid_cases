

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;
public class WriteToDB {


public String tablname;
public WriteToDB(String tablename){
    this.tablname=tablename;
}


    private static final String url = "jdbc:mysql://localhost:3306/coviddb";

    public void writeToMySql(Dataset<Row> covid)  {
    ConnectToDb pr = new ConnectToDb();
    Properties properties = pr.connect();


    covid.write().mode(SaveMode.Overwrite).jdbc(url,tablname,properties);

    }

}
