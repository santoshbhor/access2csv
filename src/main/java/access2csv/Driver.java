package access2csv;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import com.opencsv.CSVWriter;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import com.healthmarketscience.jackcess.*;
import com.healthmarketscience.jackcess.Database.FileFormat;
import com.healthmarketscience.jackcess.util.ImportFilter;
import com.healthmarketscience.jackcess.util.ImportUtil;
import com.healthmarketscience.jackcess.util.SimpleImportFilter;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

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

	static void importCSV(final File inputFile, final File dbFile, String delimiter) throws IOException {
		final Database db = dbFile.exists() ? DatabaseBuilder.open(dbFile)
				: DatabaseBuilder.create(FileFormat.V2016, dbFile);

		try {
			if (delimiter.isEmpty()) {
				delimiter = ",";
			}
			ImportFilter filter = new SimpleImportFilter() {				
				@Override
				public List<ColumnBuilder> filterColumns(final List<ColumnBuilder> destColumns,
				final ResultSetMetaData srcColumns) throws SQLException, IOException {
					System.out.println("Converting all Text Fields to Memo fields for maximum length!");
					StringBuilder cols = new StringBuilder();
					for (final ColumnBuilder column : destColumns) {
						cols.append(column.getName() + ",");
						// map all TEXT fields to Type MEMO to allow max length allowed by java
						if (column.getType().compareTo(DataType.TEXT) == 0) {
							column.setType(DataType.MEMO);
							column.setMaxLength();
						}
					}
					System.out.println("Header Columns: " + StringUtils.stripEnd(cols.toString(),","));
					return destColumns;
				}
			};
			new ImportUtil.Builder(db, getFileNameWithoutExtension(inputFile)).setDelimiter(delimiter).setFilter(filter)
					.importFile(inputFile);
		} finally {
			db.close();
		}
	}

	static int export(final Database db, final String tableName, final Writer csv, final boolean withHeader,
			final boolean applyQuotesToAll, final String nullText) throws IOException {
		final Table table = db.getTable(tableName);
		final String[] buffer = new String[table.getColumnCount()];
		final CSVWriter writer = new CSVWriter(new BufferedWriter(csv));
		// upgraded csvwrited to latest version ... new CSVWriter(new
		// BufferedWriter(csv),
		// CSVWriter.DEFAULT_SEPARATOR,CSVWriter.DEFAULT_QUOTE_CHARACTER,
		// CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
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
		final OptionSpec<String> importdelimiter = parser.accepts("import-delimiter").withOptionalArg()
				.ofType(String.class).defaultsTo(",")
				.describedAs("Data Delimiter for importing (input) csv file. If not provided defaults to ','");
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
				final String _extension = FilenameUtils.getExtension(outputDir.getName());
				final boolean _outputIsMdbFile = _extension.equalsIgnoreCase("mdb")
						|| _extension.equalsIgnoreCase("accdb");
				final boolean _outputFileExists = outputDir.exists();
				System.out.println("OutPut FileName: " + outputDir.getName());
				System.out.println("OutPut Extension: " + _extension);
				System.out.println("OutPut IsMdb: " + _outputIsMdbFile);
				System.out.println("OutPut FileExists: " + _outputFileExists);

				if (_outputIsMdbFile && _outputFileExists) {
					// output is a mdb file and exists
					dbFile = outputDir;
				}

				if (_outputIsMdbFile && !_outputFileExists) {
					// if extension is accdb or mdb but file doesnot exists
					final String _filePath = outputDir.getCanonicalPath();
					dbFile = new File(FilenameUtils.removeExtension(_filePath) + ".accdb");
				}

				// if out is an empty extension aka not a file
				if (!_outputIsMdbFile && _extension.equalsIgnoreCase("")) {
					// output is not a file but a target directory create a new file and set it as
					// accdb
					final String _filePath = outputDir.getCanonicalPath();
					dbFile = new File(FilenameUtils.removeExtension(_filePath) + "output.accdb");
				}
				System.out.println("Importing data into mdb started!");
				System.out.println("InputFile :" + inputFile);
				System.out.println("OutPut MDB: " + dbFile.getName());
				final String _delimiter = importdelimiter.value(options);
				importCSV(inputFile, dbFile, _delimiter);
				System.out.println("Importing data into mdb completed!");
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
