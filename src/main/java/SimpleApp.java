import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

public class SimpleApp {
    static String sparkMaster;
    static String esMaster;
    static String[] tables;
    static String[] schemas;
    static String query;
    static String[] extJars;

    static void printUsage() throws IOException {
	CmdParser.printUsage("runEsSpark", CmdParser.getCmdOptions(), System.out);
    }

    public static void main(String[] args) throws ParseException, IOException {
	if (args.length < 4) {
	    CmdParser.printUsage("esSparkRun", CmdParser.getCmdOptions(), System.out);
	    return;
	}
	for (String s : args) {
	    System.out.println(s);
	}

	CommandLine cmd = CmdParser.getCMD(args);
	if (cmd.hasOption("spark")) {
	    sparkMaster = cmd.getOptionValue("spark");
	}
	if (cmd.hasOption("esmaster")) {
	    esMaster = cmd.getOptionValue("esmaster");
	}
	if (cmd.hasOption("schemas")) {
	    schemas = cmd.getOptionValue("schemas").split(",");
	}
	if (cmd.hasOption("tables")) {
	    tables = cmd.getOptionValue("tables").split(",");
	}
	if (cmd.hasOption("extJars")) {
	    extJars = cmd.getOptionValue("extJars").split(",");
	}
	// if (cmd.hasOption("query")) {
	// query = cmd.getOptionValue("query");
	// }

	System.out.print("Enter query to run >> ");
	Scanner in = new Scanner(System.in);
	query = in.nextLine();
	System.out.println("Running query >>> " + query);

	if (schemas == null || schemas.length == 0) {
	    printUsage();
	    throw new IllegalArgumentException("Schema is not passed");
	}

	SparkConf conf = new SparkConf().setAppName("es").setMaster(sparkMaster).setJars(extJars);
	conf.set("es.nodes", esMaster);
	conf.set("es.nodes.discovery", "false");
	JavaSparkContext jsc = new JavaSparkContext(conf);
	SQLContext sql = new SQLContext(jsc);

	int count = 0;

	List<DataFrame> dataframes = new ArrayList<>();
	for (String schema : schemas) {
	    System.out.println("preparing read from schema: " + schema + ", table:" + tables[count]);
	    DataFrame test = JavaEsSparkSQL.esDF(sql, schema);
	    dataframes.add(test);
	    test.registerTempTable(tables[count++]);
	    test.printSchema();
	}

	DataFrame df = sql.sql(query);
	long resultCount = df.count();
	System.out.println("***************************************************");
	System.out.println("Total rows => " + resultCount);
	System.out.println("***************************************************");

	if (count < 10000) {
	    List<Row> rows = df.collectAsList();
	    for (int i = 0; i < rows.size(); i++) {
		System.out.println(rows.get(i));
		if (i != 0 && i % 50 == 0) {
		    System.out.println("Press any key to print more rows");
		    in.next();
		}
	    }
	    System.out.println();
	} else {
	    System.out.println("Number of rows more than 10K. Consider adding a limit clause in your query");
	}
    }
}
