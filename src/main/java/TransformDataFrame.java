
import static org.apache.spark.sql.functions.avg;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
public class TransformDataFrame {

    public Dataset<Row> transformToMySql(Dataset<Row> data){

        Dataset<Row> result = data.groupBy("School unit name","Gender")
        .agg(avg("elementary school cases"),avg("middle schools cases"),avg("high schools cases")).dropDuplicates();;
        return result;

    }
}
