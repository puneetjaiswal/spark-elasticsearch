import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public final class CmdParser {
    static final CommandLineParser parser = new DefaultParser();

    static CommandLine getCMD(String[] args) throws ParseException {
	return parser.parse(getCmdOptions(), args);
    }

    static Options getCmdOptions() {
	Options opts = new Options();
	opts.addOption("spark", true, "spark master url => spark://osxltpjais.corp.appdynamics.com:7077");
	opts.addOption("esmaster", true, "es master url => hostname:9200");
	Option schemas = new Option("schemas", null, true, "elasticsearch schemas => index/doctype");
	Option tables = new Option("tables", null, true, "elasticsearch table names for corresponding schema => table1");
	opts.addOption(schemas);
	opts.addOption(tables);
	opts.addOption("query", true, "query \"select * from table1\"");
	opts.addOption("extJars", true, "external jars");
	return opts;
    }

    public static void printUsage(final String applicationName, final Options options, final OutputStream out)
	    throws IOException {
	final PrintWriter writer = new PrintWriter(out);
	final HelpFormatter usageFormatter = new HelpFormatter();
	usageFormatter.printUsage(writer, 80, applicationName, options);
	writer.close();
	out.close();
    }
}
