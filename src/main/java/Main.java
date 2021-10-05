import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.xml.crypto.Data;


public class Main {

public static void main(String[] args)  {
        System.setProperty("hadoop.home.dir","C://hadoop");
        SparkSession spark = SparkSession.builder().master("local")
                .appName("Java Spark SQL")
                .getOrCreate();
         String url1="src/main/resources/MOCK_DATA.csv";
         String url2="src/main/resources/MOCK_DATA_2.csv";
    ReadAndShow rd = new ReadAndShow(url1);
    Dataset<Row> data = rd.read_show(spark);
    ReadAndShow rd2 = new ReadAndShow(url2);
    Dataset<Row> data2 = rd2.read_show(spark);
    TransformDataFrame tr = new TransformDataFrame();
    Dataset<Row> transformed = tr.transformToMySql(data);
    TransformDataFrame tr2 = new TransformDataFrame();
    Dataset<Row> transformed2 = tr2.transformToMySql(data2);
    WriteToDB wr = new WriteToDB("covidcase");
    wr.writeToMySql(transformed);
    ReadFromDB rdb = new ReadFromDB("covidcase");
    Dataset<Row> readata = rdb.readFromMySql(spark);
    JoinDataFrames jdb = new JoinDataFrames();
    Dataset<Row> joined = jdb.join2Data(readata,transformed2);
    joined.show();
    WriteToDB wr1 = new WriteToDB("covidcase2");
    wr1.writeToMySql(joined);
       spark.stop();
    }


}