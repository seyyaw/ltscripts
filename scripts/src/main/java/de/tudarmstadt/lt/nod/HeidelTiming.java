package de.tudarmstadt.lt.nod;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;

import org.apache.commons.io.IOUtils;

import de.unihd.dbs.heideltime.standalone.CLISwitch;
import de.unihd.dbs.heideltime.standalone.Config;
import de.unihd.dbs.heideltime.standalone.DocumentType;
import de.unihd.dbs.heideltime.standalone.HeidelTimeStandalone;
import de.unihd.dbs.heideltime.standalone.OutputType;
import de.unihd.dbs.heideltime.standalone.POSTagger;
import de.unihd.dbs.uima.annotator.heideltime.resources.Language;
import de.unihd.dbs.uima.annotator.heideltime.resources.ResourceScanner;

public class HeidelTiming {
	private static Connection conn;
	private static Statement st;

	public static void main(String[] arg) throws ParseException, InstantiationException, IllegalAccessException,
			ClassNotFoundException, SQLException, FileNotFoundException {
		String[] args = new String[] { "-c","config.props","-t","news","-o","newsleak" };
		FileOutputStream os = new FileOutputStream("heideltime.tsv");
		for (int i = 0; i < args.length; i++) {
			if (args[i].startsWith("-")) {
				CLISwitch sw = CLISwitch.getEnumFromSwitch(args[i]);
				if (sw == null) { // unsupported CLI switch
					System.exit(-1);
				}

				if (sw.getHasFollowingValue()) { // handle values for
													// switches
					if (args.length > i + 1 && !args[i + 1].startsWith("-")) {
						sw.setValue(args[++i]);
					} else { // value is missing or malformed
						System.exit(-1);
					}
				} else { // activate the value-less switches
					sw.setValue(null);
				}
			}
		}

		// display help dialog if HELP-switch is given
		if (CLISwitch.HELP.getIsActive()) {
			printHelp();
			System.exit(0);
		}
		long startTime = System.currentTimeMillis();
		ExtractKeywords.initDB("cable","","","");
		st.setFetchSize(50);
		ResultSet docSt = st.executeQuery("select * from document;");
		int counter = 0;
		while (docSt.next()) {
			counter++;
			if (counter % 100 == 0) {
				System.out.println(counter);
			}
			String content = docSt.getString("content");
			Date created = docSt.getDate("created");
			Long id = docSt.getLong("id");

			// Check output format
			OutputType outputType = null;
			if (CLISwitch.OUTPUTTYPE.getIsActive()) {
				outputType = OutputType.valueOf(CLISwitch.OUTPUTTYPE.getValue().toString().trim().toUpperCase());
			} else {
				// Output type not found
				outputType = (OutputType) CLISwitch.OUTPUTTYPE.getValue();
			}

			// Check language
			Language language = null;
			if (CLISwitch.LANGUAGE.getIsActive()) {
				language = Language.getLanguageFromString((String) CLISwitch.LANGUAGE.getValue());

				if (language == Language.WILDCARD
						&& !ResourceScanner.getInstance().getDetectedResourceFolders().contains(language.getName())) {
					printHelp();
					System.exit(-1);
				} else {
				}
			} else {
				// Language not found
				language = Language.getLanguageFromString((String) CLISwitch.LANGUAGE.getValue());
				;
			}

			// Check type
			DocumentType type = null;
			if (CLISwitch.DOCTYPE.getIsActive()) {
				try {
					if (CLISwitch.DOCTYPE.getValue().equals("narrative")) {
						CLISwitch.DOCTYPE.setValue("narratives");
					}
					type = DocumentType.valueOf(CLISwitch.DOCTYPE.getValue().toString().toUpperCase());
				} catch (IllegalArgumentException e) {
					System.exit(-1);
				}
			} else {
				// Type not found
				type = (DocumentType) CLISwitch.DOCTYPE.getValue();
			}

			// Check document creation time
			Date dct = null;
			try {
				DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
				dct = formatter.parse(created.toString());
			} catch (Exception e) {
				printHelp();
				System.exit(-1);
			}

			// Handle locale switch
			String locale = (String) CLISwitch.LOCALE.getValue();
			Locale myLocale = null;
			if (CLISwitch.LOCALE.getIsActive()) {
				// check if the requested locale is available
				for (Locale l : Locale.getAvailableLocales()) {
					if (l.toString().toLowerCase().equals(locale.toLowerCase()))
						myLocale = l;
				}

				try {
					Locale.setDefault(myLocale); // try to set the locale
				} catch (Exception e) {
					printHelp();
					System.exit(-1);
				}
			}

			// Read configuration from file
			String configPath = CLISwitch.CONFIGFILE.getValue().toString();
			try {
				readConfigFile(configPath);
			} catch (Exception e) {
				printHelp();
				System.exit(-1);
			}

			// Set the preprocessing POS tagger
			POSTagger posTagger = null;
			if (CLISwitch.POSTAGGER.getIsActive()) {
				try {
					posTagger = POSTagger.valueOf(CLISwitch.POSTAGGER.getValue().toString().toUpperCase());
				} catch (IllegalArgumentException e) {
					printHelp();
					System.exit(-1);
				}
			} else {
				// Type not found
				posTagger = (POSTagger) CLISwitch.POSTAGGER.getValue();
			}

			// Set whether or not to use the Interval Tagger
			Boolean doIntervalTagging = false;
			if (CLISwitch.INTERVALS.getIsActive()) {
				doIntervalTagging = CLISwitch.INTERVALS.getIsActive();
			} else {
			}

			try {

				HeidelTimeStandalone standalone = new HeidelTimeStandalone(language, type, outputType, null, posTagger,
						doIntervalTagging);
				String out = standalone.process(content, dct);
				for (String line : out.split("\n")) {
					IOUtils.write(id + "\t" + line + "\n", os, "UTF-8");
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		conn.close();
		long endTime = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println("Total time in second = " + (double) totalTime / 1000);
	}

	private static void printHelp() {
		String path = HeidelTimeStandalone.class.getProtectionDomain().getCodeSource().getLocation().getFile();
		String filename = path.substring(path.lastIndexOf(System.getProperty("file.separator")) + 1);

		System.out.println("HeidelTime Standalone");
		System.out.println("Copyright © 2011-2015 Jannik Strötgen");
		System.out.println("This software is free. See the COPYING file for copying conditions.");
		System.out.println();

		System.out.println("Usage:");
		System.out.println("  java -jar " + filename + " <input-document> [-param1 <value1> ...]");
		System.out.println();
		System.out.println("Parameters and expected values:");
		for (CLISwitch c : CLISwitch.values()) {
			System.out.println(
					"  " + c.getSwitchString() + "\t" + ((c.getSwitchString().length() > 4) ? "" : "\t") + c.getName());

			if (c == CLISwitch.LANGUAGE) {
				System.out.print("\t\t" + "Available languages: [ ");
				for (Language l : Language.values())
					if (l != Language.WILDCARD)
						System.out.print(l.getName().toLowerCase() + " ");
				System.out.println("]");
			}

			if (c == CLISwitch.POSTAGGER) {
				System.out.print("\t\t" + "Available taggers: [ ");
				for (POSTagger p : POSTagger.values())
					System.out.print(p.toString().toLowerCase() + " ");
				System.out.println("]");
			}

			if (c == CLISwitch.DOCTYPE) {
				System.out.print("\t\t" + "Available types: [ ");
				for (DocumentType t : DocumentType.values())
					System.out.print(t.toString().toLowerCase() + " ");
				System.out.println("]");
			}
		}

		System.out.println();
	}

	public static void readConfigFile(String configPath) {
		InputStream configStream = null;
		try {
			configStream = new FileInputStream(configPath);

			Properties props = new Properties();
			props.load(configStream);

			Config.setProps(props);

			configStream.close();
		} catch (FileNotFoundException e) {

			System.exit(-1);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
