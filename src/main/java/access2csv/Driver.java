package access2csv;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.List;

import com.opencsv.CSVWriter;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import com.healthmarketscience.jackcess.*;
import com.healthmarketscience.jackcess.Database.FileFormat;
import com.healthmarketscience.jackcess.util.ImportUtil;
import org.apache.commons.io.FilenameUtils;

public class Driver {

	static String getFileNameWithoutExtension(final File file) {
		String fileName = "";

		try {
			if (file != null && file.exists()) {
				final String name = file.getName();
				fileName = name.replaceFirst("[.][^.]+$", "");
			}
		} catch (final Exception e) {
			e.printStackTrace();
			fileName = "";
		}

		return fileName;

	}

	static void importCSV(final File inputFile, final File dbFile) throws IOException {
		final Database db = DatabaseBuilder.create(FileFormat.V2000, dbFile);
		try {
			System.out.println("InputFile :" + inputFile);
			System.out.println("OutputFile (db) :" + dbFile);
			new ImportUtil.Builder(db, getFileNameWithoutExtension(inputFile)).setDelimiter(",").importFile(inputFile);
			System.out.println("Import completed!");
		} finally {
			db.close();
		}
	}

	static int export(final Database db, final String tableName, final Writer csv, final boolean withHeader,
			final boolean applyQuotesToAll, final String nullText) throws IOException {
		final Table table = db.getTable(tableName);
		final String[] buffer = new String[table.getColumnCount()];
		final CSVWriter writer = new CSVWriter(new BufferedWriter(csv), CSVWriter.DEFAULT_SEPARATOR,
				CSVWriter.DEFAULT_QUOTE_CHARACTER);
		int rows = 0;
		try {
			if (withHeader) {
				int x = 0;
				for (final Column col : table.getColumns()) {
					buffer[x++] = col.getName();
				}
				writer.writeNext(buffer, applyQuotesToAll);
			}

			for (final Row row : table) {
				int i = 0;
				for (final Object object : row.values()) {
					buffer[i++] = object == null ? nullText : object.toString();
				}
				writer.writeNext(buffer, applyQuotesToAll);
				rows++;
			}
		} finally {
			writer.close();
		}
		return rows;
	}

	static void export(final File inputFile, final String tableName, final boolean withHeader, final File outputDir,
			final String csvPrefix, final boolean applyQuotesToAll, final String nullText) throws IOException {
		final Database db = DatabaseBuilder.open(inputFile);
		try {
			export(db, tableName, new FileWriter(new File(outputDir, csvPrefix + tableName + ".csv")), withHeader,
					applyQuotesToAll, nullText);
		} finally {
			db.close();
		}
	}

	static void schema(final File inputFile) throws IOException {

		final Database db = DatabaseBuilder.open(inputFile);
		try {
			for (final String tableName : db.getTableNames()) {
				final Table table = db.getTable(tableName);
				System.out.println(String.format("CREATE TABLE %s (", tableName));
				for (final Column col : table.getColumns()) {
					System.out.println(String.format("  %s %s,", col.getName(), col.getType()));
				}
				System.out.println(")");
			}
		} finally {
			db.close();
		}

	}

	static void exportAll(final File inputFile, final boolean withHeader, final File outputDir, final String csvPrefix,
			final boolean applyQuotesToAll, final String nullText) throws IOException {
		final Database db = DatabaseBuilder.open(inputFile);
		try {
			for (final String tableName : db.getTableNames()) {
				final String csvName = csvPrefix + tableName + ".csv";
				final File outputFile = new File(outputDir, csvName);
				final Writer csv = new FileWriter(outputFile);
				try {
					System.out.println(String.format("Exporting '%s' to %s", tableName, outputFile.toString()));
					final int rows = export(db, tableName, csv, withHeader, applyQuotesToAll, nullText);
					System.out.println(String.format("%d rows exported", rows));
				} finally {
					try {
						csv.flush();
						csv.close();
					} catch (final IOException ex) {
					}
				}
			}
		} finally {
			db.close();
		}

	}

	public static void main(final String[] args) throws Exception {
		final OptionParser parser = new OptionParser();

		final OptionSpec<Void> help = parser.acceptsAll(Arrays.asList("help")).forHelp();
		final OptionSpec<String> schema = parser.accepts("schema").withOptionalArg()
				.describedAs("The schema is written to standard output.");
		final OptionSpec<String> importcsv = parser.accepts("import").withOptionalArg()
				.describedAs("When import is included, the given csv input file is imported into the output mdb file.");
		final OptionSpec<String> withHeader = parser.accepts("with-header").withOptionalArg().describedAs(
				"When with-header is included, a header line of column names is written to each data file.");
		final OptionSpec<File> input = parser.accepts("input").withRequiredArg().ofType(File.class).required()
				.describedAs("The input accdb file.");
		final OptionSpec<String> table = parser.accepts("table").withRequiredArg().ofType(String.class)
				.describedAs("The table name to export, or all if it is not specified.");
		final OptionSpec<File> output = parser.accepts("output").requiredUnless("schema").withRequiredArg()
				.ofType(File.class).describedAs(
						"The output directory for data files. This is required for writing data output. This not required for schema output.");
		final OptionSpec<String> csvPrefix = parser.accepts("csv-prefix").withRequiredArg().ofType(String.class)
				.defaultsTo("").describedAs("A prefix to add to all of the generated CSV file names");
		final OptionSpec<Boolean> quoteAll = parser.accepts("quote-all").withOptionalArg().ofType(Boolean.class)
				.defaultsTo(true)
				.describedAs("Set quote-all to true if all values are to be quoted. "
						+ "Set to false if quotes are only to be applied to values which contain "
						+ "the separator, secape, quote, or new line characters. The default is true.");
		final OptionSpec<String> writeNull = parser.accepts("write-null").withOptionalArg().ofType(String.class)
				.defaultsTo("").describedAs(
						"The text to write when entry is NULL. Defaults to empty output if not specified or if no argument supplied. "
								+ "If quote-all is set to true then the value for write-null is also quoted.");

		OptionSet options = null;

		try {
			options = parser.parse(args);
		} catch (final OptionException e) {
			System.out.println(e.getMessage());
			parser.printHelpOn(System.out);
			throw e;
		}

		if (options.has(help)) {
			parser.printHelpOn(System.out);
			return;
		}

		final File inputFile = input.value(options);
		if (!inputFile.exists()) {
			throw new FileNotFoundException("Could not find input file: " + inputFile.toString());
		}

		if (options.has(importcsv)) {
			// process imports
			File outputDir = null;
			File dbFile = null;
			if (options.has(output)) {
				outputDir = output.value(options);
				if (outputDir.isFile()) {
					// output is a file can be a mdb file
					final String _extension = FilenameUtils.getExtension(outputDir.getName());
					if (_extension.equalsIgnoreCase("mdb") || _extension.equalsIgnoreCase("accdb")) {
						dbFile = outputDir;
					} else {
						// if extension is not accdb or mdb we create a new file with the name provided
						// but extension accdb
						final String _filePath = outputDir.getCanonicalPath();
						dbFile = new File(FilenameUtils.removeExtension(_filePath) + ".accdb");
					}
				} else {
					// output is not a file but a target directory create a new file and set it as
					// accdb
					final String _filePath = outputDir.getCanonicalPath();
					dbFile = new File(FilenameUtils.removeExtension(_filePath) + "output.accdb");
				}
				importCSV(inputFile, dbFile);// call inputCSV
			}

		} else {
			// process all extractions
			File outputDir = null;
			if (options.has(output)) {
				outputDir = output.value(options);
				if (!outputDir.exists()) {
					outputDir.mkdirs();
				}
			}

			final boolean applyQuotesToAll = quoteAll.value(options);
			final String nullText = writeNull.value(options);
    
		if (options.has(schema)) {
			schema(inputFile);
		}
		
		if (null != outputDir) {
			if (options.has(table)){
				export(inputFile, table.value(options), options.has(withHeader), outputDir, csvPrefix.value(options), applyQuotesToAll, nullText);
			}
			else {
				exportAll(inputFile, options.has(withHeader), outputDir, csvPrefix.value(options), applyQuotesToAll, nullText);
			}	
		
		}
	}
	
}
}
