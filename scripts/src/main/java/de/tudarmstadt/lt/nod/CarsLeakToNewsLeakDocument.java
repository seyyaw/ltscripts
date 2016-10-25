package de.tudarmstadt.lt.nod;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.common.base.Optional;
import com.opencsv.CSVWriter;
import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.CommonTextObjectFactories;
import com.optimaize.langdetect.text.TextObject;
import com.optimaize.langdetect.text.TextObjectFactory;

import de.tudarmstadt.lt.nod.model.Document;

public class CarsLeakToNewsLeakDocument {
	public static final String LF = System.getProperty("line.separator");
	public static final String TAB = "\t";

	public static void main(String[] args) throws IOException {
		JSONParser parser = new JSONParser();
		CSVWriter writerde = new CSVWriter(new FileWriter("de_document.csv"));
		CSVWriter writeren = new CSVWriter(new FileWriter("en_document.csv"));
		
		CSVWriter metadata = new CSVWriter(new FileWriter("metadata.csv"));
		
		try {

			Object obj = parser.parse(new FileReader("cars.json"));

			JSONObject jsonObject = (JSONObject) obj;
			JSONObject hitsObject = (JSONObject) jsonObject.get("hits");

			JSONArray hitsArray = (JSONArray) hitsObject.get("hits");
			int docId = 1;
			int parentDocId = 1;

			for (Iterator<?> iterator = hitsArray.iterator(); iterator.hasNext();) {
				JSONObject hitsDocument = (JSONObject) iterator.next();
				JSONObject source = (JSONObject) hitsDocument.get("_source");
				JSONObject main = (JSONObject) source.get("Main");

				String lang = (String) main.get("Language");

				JSONObject file = (JSONObject) source.get("File");
				String created = (String) file.get("created");
				String content = ((String) main.get("Content"));

				LineIterator it = IOUtils.lineIterator(new StringReader(content));
				int lineCount = 0;
				StringBuffer sb = new StringBuffer();

				while (it.hasNext()) {
					String line = it.nextLine();
					if (line.isEmpty()) {
						continue;
					}
					if (lineCount >= 1000) {
						if(languageDetector(sb.toString()).equals("de")){
							String[] entries = new String[] { docId + "", StringEscapeUtils.unescapeCsv(sb.toString().replace("\n", "<br>")), created };
							writerde.writeNext(entries);
							writeMetadata(metadata, docId, file, parentDocId);
						}
						else{
							String[] entries = new String[] { docId + "", StringEscapeUtils.unescapeCsv(sb.toString().replace("\n", "<br>")), created };
							writeren.writeNext(entries);	
							writeMetadata(metadata, docId, file, parentDocId);
						}
						sb = new StringBuffer();
						lineCount = 0;
						docId ++;
					} else {
						sb.append(line + "\n");
						lineCount++;
					}

				}
				

				parentDocId++;
			}
			writerde.close();
			writeren.close();
			metadata.close();
			/*
			 * for(Iterator<?> iterator = hitsHits.keySet().iterator();
			 * iterator.hasNext();) { String key = (String) iterator.next();
			 * System.out.println(key); }
			 */

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}

	}

	private static void writeMetadata(CSVWriter metadata, int docId, JSONObject file, int paranetDocId) {
		String filename = (String) file.get("filename");
		String uuid = (String) file.get("uuid");
		String[] fileMetadata = new String[] { docId + "", "Filename", filename, "Text" };
		metadata.writeNext(fileMetadata);
		String[] uuidMetadata = new String[] { docId + "", "Uuid", uuid, "Text" };
		metadata.writeNext(uuidMetadata);		
		String[] parentIdMetadata = new String[] { docId + "", "paranetDocId", paranetDocId+"", "Integer" };
		metadata.writeNext(parentIdMetadata);
	}

	static String languageDetector(String text) throws IOException {

		//System.out.println(text);
		List<LanguageProfile> languageProfiles = new LanguageProfileReader().readAllBuiltIn();

		// build language detector:
		LanguageDetector languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
				.withProfiles(languageProfiles).build();

		// create a text object factory
		TextObjectFactory textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText();

		// query:
		TextObject textObject = textObjectFactory.forText(text);
		Optional<LdLocale> lang = languageDetector.detect(textObject);

		if (lang.isPresent()) {
			return lang.get().getLanguage();
		} else
			return "de";
	}

	static void printKeys(JSONObject obeject) {
		for (Iterator<?> iterator = obeject.keySet().iterator(); iterator.hasNext();) {
			String key = (String) iterator.next();
			System.out.println(key);
		}
	}

	
	  /* private static String replaceEscapeChars(String annotation)
	    {
	        return annotation.replace("\\", "\\\\").replace(",", "\\,").replace("\"", "\\\"]");
	    }*/
	
}
