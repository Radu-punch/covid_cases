import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class JoinDataFrames {
    public Dataset<Row> join2Data(Dataset<Row> covid1,Dataset<Row> covid2){
       Dataset<Row> joinedata;
        joinedata = covid1.union(covid2).dropDuplicates();
        return joinedata;
    }
}
