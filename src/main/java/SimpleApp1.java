import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

public class SimpleApp1 {

    public static void main(String[] args) {

	String jars[] = { "/opt/apps/spark/lib/elasticsearch-hadoop-2.2.0.BUILD-SNAPSHOT.jar",
		"/opt/apps/spark/lib/elasticsearch-spark_2.10-2.2.0.BUILD-20150707.024158-13.jar" };

	SparkConf conf = new SparkConf().setAppName("es").setMaster("spark://ec2-52-11-99-232.us-west-2.compute.amazonaws.com:7077").setJars(jars);
	conf.set("es.nodes", "ec2-52-11-99-232.us-west-2.compute.amazonaws.com:9200");
	conf.set("es.nodes.discovery", "false");
	JavaSparkContext jsc = new JavaSparkContext(conf);
	SQLContext sql = new SQLContext(jsc);

	DataFrame test = JavaEsSparkSQL.esDF(sql, "perf_static_index/browserrecord");
	test.registerTempTable("br");
	test.printSchema();

	DataFrame df = sql.sql("select * from br");
	long resultCount = df.count();
	System.out.println("***************************************************");
	System.out.println(resultCount);
	System.out.println(df.collectAsList());
	System.out.println("***************************************************");
    }
}
