package de.tudarmstadt.lt.nod;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.joda.time.LocalDateTime;

import de.unihd.dbs.heideltime.standalone.CLISwitch;
import de.unihd.dbs.heideltime.standalone.Config;
import de.unihd.dbs.heideltime.standalone.DocumentType;
import de.unihd.dbs.heideltime.standalone.HeidelTimeStandalone;
import de.unihd.dbs.heideltime.standalone.OutputType;
import de.unihd.dbs.heideltime.standalone.POSTagger;
import de.unihd.dbs.uima.annotator.heideltime.resources.Language;
import de.unihd.dbs.uima.annotator.heideltime.resources.ResourceScanner;


public class HeidelTiming {
	private static Logger logger = Logger.getLogger("HeidelTimeStandalone");

	public static void main(String[] args) throws ParseException {
		String docPath = null;
		for(int i = 0; i < args.length; i++) { // iterate over cli parameter tokens
			if(args[i].startsWith("-")) { // assume we found a switch
				// get the relevant enum
				CLISwitch sw = CLISwitch.getEnumFromSwitch(args[i]);
				if(sw == null) { // unsupported CLI switch
					logger.log(Level.WARNING, "Unsupported switch: "+args[i]+". Quitting.");
					System.exit(-1);
				}
				
				if(sw.getHasFollowingValue()) { // handle values for switches
					if(args.length > i+1 && !args[i+1].startsWith("-")) { // we still have an array index after this one and it's not a switch
						sw.setValue(args[++i]);
					} else { // value is missing or malformed
						logger.log(Level.WARNING, "Invalid or missing parameter after "+args[i]+". Quitting.");
						System.exit(-1);
					}
				} else { // activate the value-less switches
					sw.setValue(null);
				}
			} else { // assume we found the document's path/name
				docPath = args[i];
			}
		}
		
		
		// display help dialog if HELP-switch is given
		if(CLISwitch.HELP.getIsActive()) {
			printHelp();
			System.exit(0);
		}
		
		// start off with the verbosity recognition -- lots of the other 
		// stuff can be skipped if this is set too high
		if(CLISwitch.VERBOSITY2.getIsActive()) {
			logger.setLevel(Level.ALL);
			logger.log(Level.INFO, "Verbosity: '-vv'; Logging level set to ALL.");
			
			// output the found language resource folders
			String languagesList = "";
			for(String language : ResourceScanner.getInstance().getDetectedResourceFolders()) {
				languagesList += System.getProperty("line.separator") + "- " + language;
			}
			logger.log(Level.INFO, "Listing detected language folders:" + languagesList);
		} else if(CLISwitch.VERBOSITY.getIsActive()) {
			logger.setLevel(Level.INFO);
			logger.log(Level.INFO, "Verbosity: '-v'; Logging level set to INFO and above.");
		} else {
			logger.setLevel(Level.WARNING);
			logger.log(Level.INFO, "Verbosity -v/-vv NOT FOUND OR RECOGNIZED; Logging level set to WARNING and above.");
		}
		
		// Check input encoding
		String encodingType = null;
		if(CLISwitch.ENCODING.getIsActive()) {
			encodingType = CLISwitch.ENCODING.getValue().toString();
			logger.log(Level.INFO, "Encoding '-e': "+encodingType);
		} else {
			// Encoding type not found
			encodingType = CLISwitch.ENCODING.getValue().toString();
			logger.log(Level.INFO, "Encoding '-e': NOT FOUND OR RECOGNIZED; set to 'UTF-8'");
		}
		
		// Check output format
		OutputType outputType = null;
		if(CLISwitch.OUTPUTTYPE.getIsActive()) {
			outputType = OutputType.valueOf(CLISwitch.OUTPUTTYPE.getValue().toString().toUpperCase());
			logger.log(Level.INFO, "Output '-o': "+outputType.toString().toUpperCase());
		} else {
			// Output type not found
			outputType = (OutputType) CLISwitch.OUTPUTTYPE.getValue();
			logger.log(Level.INFO, "Output '-o': NOT FOUND OR RECOGNIZED; set to "+outputType.toString().toUpperCase());
		}
		
		// Check language
		Language language = null;
		if(CLISwitch.LANGUAGE.getIsActive()) {
			language = Language.getLanguageFromString((String) CLISwitch.LANGUAGE.getValue());
			
			if(language == Language.WILDCARD && !ResourceScanner.getInstance().getDetectedResourceFolders().contains(language.getName())) {
				logger.log(Level.SEVERE, "Language '-l': "+CLISwitch.LANGUAGE.getValue()+" NOT RECOGNIZED; aborting.");
				printHelp();
				System.exit(-1);
			} else {
				logger.log(Level.INFO, "Language '-l': "+language.getName());	
			}
		} else {
			// Language not found
			language = Language.getLanguageFromString((String) CLISwitch.LANGUAGE.getValue());
			logger.log(Level.INFO, "Language '-l': NOT FOUND; set to "+language.toString().toUpperCase());
		}

		// Check type
		DocumentType type = null;
		if(CLISwitch.DOCTYPE.getIsActive()) {
			try {
				if(CLISwitch.DOCTYPE.getValue().equals("narrative")) { // redirect "narrative" to "narratives"
					CLISwitch.DOCTYPE.setValue("narratives");
				}
				type = DocumentType.valueOf(CLISwitch.DOCTYPE.getValue().toString().toUpperCase());
			} catch(IllegalArgumentException e) {
				logger.log(Level.WARNING, "Type '-t': NOT RECOGNIZED. These are the available options: " + Arrays.asList(DocumentType.values()));
				System.exit(-1);
			}
			logger.log(Level.INFO, "Type '-t': "+type.toString().toUpperCase());
		} else {
			// Type not found
			type = (DocumentType) CLISwitch.DOCTYPE.getValue();
			logger.log(Level.INFO, "Type '-t': NOT FOUND; set to "+type.toString().toUpperCase());
		}

		// Check document creation time
		Date dct = null;
		if(CLISwitch.DCT.getIsActive()) {
			try {
				DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
				dct = formatter.parse(CLISwitch.DCT.getValue().toString());
				logger.log(Level.INFO, "Document Creation Time '-dct': "+dct.toString());
			} catch (Exception e) {
				// DCT was not parseable
				logger.log(Level.WARNING, "Document Creation Time '-dct': NOT RECOGNIZED. Quitting.");
				printHelp();
				System.exit(-1);
			}
		} else {
			if ((type == DocumentType.NEWS) || (type == DocumentType.COLLOQUIAL)) {
				// Dct needed
				dct = (Date) CLISwitch.DCT.getValue();
				logger.log(Level.INFO, "Document Creation Time '-dct': NOT FOUND; set to local date ("
						+ dct.toString() + ").");
			} else {
				logger.log(Level.INFO, "Document Creation Time '-dct': NOT FOUND; skipping.");
			}
		}
		
		// Handle locale switch
		String locale = (String) CLISwitch.LOCALE.getValue();
		Locale myLocale = null;
		if(CLISwitch.LOCALE.getIsActive()) {
			// check if the requested locale is available
			for(Locale l : Locale.getAvailableLocales()) {
				if(l.toString().toLowerCase().equals(locale.toLowerCase()))
					myLocale = l;
			}
			
			try {
				Locale.setDefault(myLocale); // try to set the locale
				logger.log(Level.INFO, "Locale '-locale': "+myLocale.toString());
			} catch(Exception e) { // if the above fails, spit out error message and available locales
				logger.log(Level.WARNING, "Supplied locale parameter couldn't be resolved to a working locale. Try one of these:");
				logger.log(Level.WARNING, Arrays.asList(Locale.getAvailableLocales()).toString()); // list available locales
				printHelp();
				System.exit(-1);
			}
		} else {
			// no -locale parameter supplied: just show default locale
			logger.log(Level.INFO, "Locale '-locale': NOT FOUND, set to environment locale: "+Locale.getDefault().toString());
		}
		
		// Read configuration from file
		String configPath = CLISwitch.CONFIGFILE.getValue().toString();
		try {
			logger.log(Level.INFO, "Configuration path '-c': "+configPath);

			readConfigFile(configPath);

			logger.log(Level.FINE, "Config initialized");
		} catch (Exception e) {
			e.printStackTrace();
			logger.log(Level.WARNING, "Config could not be initialized! Please supply the -c switch or "
					+ "put a config.props into this directory.");
			printHelp();
			System.exit(-1);
		}

		// Set the preprocessing POS tagger
		POSTagger posTagger = null;
		if(CLISwitch.POSTAGGER.getIsActive()) {
			try {
				posTagger = POSTagger.valueOf(CLISwitch.POSTAGGER.getValue().toString().toUpperCase());
			} catch(IllegalArgumentException e) {
				logger.log(Level.WARNING, "Given POS Tagger doesn't exist. Please specify a valid one as listed in the help.");
				printHelp();
				System.exit(-1);
			}
			logger.log(Level.INFO, "POS Tagger '-pos': "+posTagger.toString().toUpperCase());
		} else {
			// Type not found
			posTagger = (POSTagger) CLISwitch.POSTAGGER.getValue();
			logger.log(Level.INFO, "POS Tagger '-pos': NOT FOUND OR RECOGNIZED; set to "+posTagger.toString().toUpperCase());
		}

		// Set whether or not to use the Interval Tagger
		Boolean doIntervalTagging = false;
		if(CLISwitch.INTERVALS.getIsActive()) {
			doIntervalTagging = CLISwitch.INTERVALS.getIsActive();
			logger.log(Level.INFO, "Interval Tagger '-it': " + doIntervalTagging.toString());
		} else {
			logger.log(Level.INFO, "Interval Tagger '-it': NOT FOUND OR RECOGNIZED; set to " + doIntervalTagging.toString());
		}
		
		// make sure we have a document path
		if (docPath == null) {
			logger.log(Level.WARNING, "No input file given; aborting.");
			printHelp();
			System.exit(-1);
		}
		
		

		// Run HeidelTime
		RandomAccessFile aFile = null;
		MappedByteBuffer buffer = null;
		FileChannel inChannel = null;
		PrintWriter pwOut = null;
		try {
			logger.log(Level.INFO, "Reading document using charset: " + encodingType);
			
			aFile = new RandomAccessFile(docPath, "r");
			inChannel = aFile.getChannel();
			buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size());
			buffer.load();
			byte[] inArr = new byte[(int) inChannel.size()];
			
			for(int i = 0; i < buffer.limit(); i++) {
				inArr[i] = buffer.get();
			}
			
			// double-newstring should not be necessary, but without this, it's not running on Windows (?)
			String input = new String(new String(inArr, encodingType).getBytes("UTF-8"), "UTF-8");
			
			HeidelTimeStandalone standalone = new HeidelTimeStandalone(language, type, outputType, null, posTagger, doIntervalTagging);
			String out = standalone.process(input, dct);
			
			// Print output always as UTF-8
			pwOut = new PrintWriter(new OutputStreamWriter(System.out, "UTF-8"));
			pwOut.println(out);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(pwOut != null) {
				pwOut.close();
			}
			if(buffer != null) {
				buffer.clear();
			}
			if(inChannel != null) {
				try {
					inChannel.close();
				} catch (IOException e) { }
			}
			if(aFile != null) {
				try {
					aFile.close();
				} catch (IOException e) { }
			}
		}
	}
	private static void printHelp() {
		String path = HeidelTimeStandalone.class.getProtectionDomain().getCodeSource().getLocation().getFile();
		String filename = path.substring(path.lastIndexOf(System.getProperty("file.separator")) + 1);

		System.out.println("HeidelTime Standalone");
		System.out.println("Copyright © 2011-2015 Jannik Strötgen");
		System.out.println("This software is free. See the COPYING file for copying conditions.");
		System.out.println();
		
		System.out.println("Usage:");
		System.out.println("  java -jar " 
				+ filename 
				+ " <input-document> [-param1 <value1> ...]");
		System.out.println();
		System.out.println("Parameters and expected values:");
		for(CLISwitch c : CLISwitch.values()) {
			System.out.println("  " 
					+ c.getSwitchString() 
					+ "\t"
					+ ((c.getSwitchString().length() > 4)? "" : "\t")
					+ c.getName()
					);

			if(c == CLISwitch.LANGUAGE) {
				System.out.print("\t\t" + "Available languages: [ ");
				for(Language l : Language.values())
					if(l != Language.WILDCARD)
						System.out.print(l.getName().toLowerCase()+" ");
				System.out.println("]");
			}
			
			if(c == CLISwitch.POSTAGGER) {
				System.out.print("\t\t" + "Available taggers: [ ");
				for(POSTagger p : POSTagger.values())
					System.out.print(p.toString().toLowerCase()+" ");
				System.out.println("]");
			}
			
			if(c == CLISwitch.DOCTYPE) {
				System.out.print("\t\t" + "Available types: [ ");
				for(DocumentType t : DocumentType.values())
					System.out.print(t.toString().toLowerCase()+" ");
				System.out.println("]");
			}
		}
		
		System.out.println();
	}
	

	public static void readConfigFile(String configPath) {
		InputStream configStream = null;
		try {
			logger.log(Level.INFO, "trying to read in file "+configPath);
			configStream = new FileInputStream(configPath);
			
			Properties props = new Properties();
			props.load(configStream);

			Config.setProps(props);
			
			configStream.close();
		} catch (FileNotFoundException e) {
			logger.log(Level.WARNING, "couldn't open configuration file \""+configPath+"\". quitting.");
			System.exit(-1);
		} catch (IOException e) {
			logger.log(Level.WARNING, "couldn't close config file handle");
			e.printStackTrace();
		}
	}
}
