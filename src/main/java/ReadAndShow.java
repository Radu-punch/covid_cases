import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadAndShow {
private String url;
public ReadAndShow(String url){
    this.url=url;

}

    protected Dataset<Row> read_show(SparkSession spark){
        Dataset<Row> covid = spark.read().option("header","true").option("inferSchema","true").csv(url);
        covid.show();
        covid.printSchema();
        return covid;
    }

}
