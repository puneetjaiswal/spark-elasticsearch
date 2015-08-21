import java.io.IOException;

import org.apache.commons.cli.ParseException;

public class Main {

    public static void main(String[] args) throws ParseException, IOException {
	System.out.println("Running through launcher...");
	String[] a = {
		"-spark",
		"spark://osxltpjais.corp.appdynamics.com:7077",
		"-esmaster",
		"localhost:9200",
		"-extJars",
		"/opt/Code/my/spark-es/target/spark-es-1.0.jar,/opt/Code/my/spark-es/target/lib/spark-es-1.0/elasticsearch-hadoop-2.2.0.BUILD-20150713.154541-20.jar,/opt/Code/my/spark-es/target/spark-es-1.0/lib/elasticsearch-spark_2.10-2.2.0.BUILD-20150713.154731-20.jar",
		"-schemas", "perf_dynamic_index/biz_txn", "-tables", "test", "-query", "select * from test" };
	SimpleApp.main(a);
    }
}
