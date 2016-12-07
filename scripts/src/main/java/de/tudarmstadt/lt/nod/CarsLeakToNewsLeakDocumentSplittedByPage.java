package de.tudarmstadt.lt.nod;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

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

public class CarsLeakToNewsLeakDocumentSplittedByPage {
	public static final String LF = System.getProperty("line.separator");
	public static final String TAB = "\t";

	static CSVWriter writer;
	static CSVWriter metadata;

	static OutputStream entWriter;
	static OutputStream entDocWriter;
	static OutputStream entOffWriter;

	public static void main(String[] args) throws IOException {
		JSONParser parser = new JSONParser();
		writer = new CSVWriter(new FileWriter("document.csv"));
		metadata = new CSVWriter(new FileWriter("metadata.csv"));

		entWriter = new FileOutputStream("entity.tsv");
		entDocWriter = new FileOutputStream("entityDoc.tsv");
		entOffWriter = new FileOutputStream("entityOff.tsv");
		try {

			Object obj = parser.parse(new FileReader("/media/seid/DATA/LT/DIVID-DJ/cars/cars2/cars.json"));
			// Uncomment the following if the file is fully ES type

			/*
			 * JSONObject jsonObject = (JSONObject) obj; JSONObject hitsObject =
			 * (JSONObject) jsonObject.get("hits");
			 * 
			 * JSONArray hitsArray = (JSONArray) hitsObject.get("hits");
			 */

			JSONArray hitsArray = (JSONArray) obj;

			int docId = 1;
			int parentDocId = 1;

			Map<String, Integer> allEnts = new LinkedHashMap<>();
			Map<String, Integer> entIds = new HashMap<>();
			for (Iterator<?> iterator = hitsArray.iterator(); iterator.hasNext();) {
				System.out.println(parentDocId);
				JSONObject hitsDocument = (JSONObject) iterator.next();
				JSONObject source = (JSONObject) hitsDocument.get("_source");
				JSONObject main = (JSONObject) source.get("Main");

				String content = ((String) main.get("Content"));
				if (content == null) {
					System.out.println("Empty");
					continue;
				}
				content = content.replace("\r", "");

				LineIterator it = IOUtils.lineIterator(new StringReader(content));
				StringBuffer sb = new StringBuffer();

				String seite = "";
				boolean ordnerFound = false;
				while (it.hasNext()) {
					String line = it.nextLine();
					if (line.isEmpty()) {
						continue;
					}

					// String pattern = ".*Ordner\\s+\\d+\\s+von\\s+\\d+.*";
					String pattern = ".*Ordner\\s+\\d+\\s+.*";

					if (ordnerFound)
						seite = line;

					if (Pattern.matches(pattern, line)) {
						ordnerFound = true;
						continue;
					}

					if ((ordnerFound && !seite.isEmpty()) || sb.length()>5000) {
						if (sb.toString().isEmpty()) {
							seite = "";
							ordnerFound = false;
							continue;
						}
						writeDoc(docId, sb, hitsDocument, allEnts, parentDocId, seite, entIds);
						sb = new StringBuffer();
						docId++;
						seite = "";
						ordnerFound = false;
					} else {
						sb.append(line + "\n");
					}
				}
				
				if(sb.length()>0){
					writeDoc(docId, sb, hitsDocument, allEnts, parentDocId, seite, entIds);
					docId++;
					seite = "";
					ordnerFound = false;
				}

				parentDocId++;
			}

			for (String entAndType : allEnts.keySet()) {
				IOUtils.write(entIds.get(entAndType) + "\t" + entAndType + "\t" + allEnts.get(entAndType) + "\tfalse\n",
						entWriter, "UTF8");
			}
			writer.close();
			metadata.close();
			entWriter.close();
			entDocWriter.close();
			entOffWriter.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}

	}

	static void writeDoc(int docId, StringBuffer sb, JSONObject hitsDocument, Map<String, Integer> allEnts,
			int parentDocID, String seite, Map<String, Integer> entIds) throws IOException {

		JSONObject source = (JSONObject) hitsDocument.get("_source");
		JSONObject main = (JSONObject) source.get("Main");

		String lang = (String) main.get("Language") == null ? "de" : "en";

		JSONObject file = (JSONObject) source.get("File");
		String created = (String) file.get("created");
		Set<Integer> begins = new HashSet<>();

		String[] entries = new String[] { docId + "", sb.toString(), created };
		writer.writeNext(entries);
		String filename = (String) file.get("filename");
		String uuid = (String) file.get("uuid");

		JSONArray person = (JSONArray) main.get("PERSON");
		addEntityDocOffset(entDocWriter, entOffWriter, docId, allEnts, entIds, sb, begins, getEntities(person), "PER");
		JSONArray location = (JSONArray) main.get("LOCATION");
		addEntityDocOffset(entDocWriter, entOffWriter, docId, allEnts, entIds, sb, begins, getEntities(location),
				"LOC");
		JSONArray org = (JSONArray) main.get("ORGANIZATION");
		addEntityDocOffset(entDocWriter, entOffWriter, docId, allEnts, entIds, sb, begins, getEntities(org), "ORG");
		JSONArray misc = (JSONArray) main.get("MISC");
		addEntityDocOffset(entDocWriter, entOffWriter, docId, allEnts, entIds, sb, begins, getEntities(misc), "MISC");

		writeMetadata(metadata, docId, "Filename", filename, "Text");
		writeMetadata(metadata, docId, "ParentDocId", parentDocID, "Number");
		writeMetadata(metadata, docId, "Uuid", uuid, "Text");
		writeMetadata(metadata, docId, "Language", lang, "Text");
		writeMetadata(metadata, docId, "Seite", seite, "Text");
	}

	private static void addEntityDocOffset(OutputStream entDocWriter, OutputStream entOffsetWriter, int docId,
			Map<String, Integer> allEnts, Map<String, Integer> entIds, StringBuffer sb, Set<Integer> begins,
			List<String> entList, String type) throws IOException {

		for (String ent : entList) {
			if (!StringUtils.isAlphanumeric(ent.replace(" ", "").replace("-", "").replace(";", "").replace(".", "")
					.replace("&", "").replace("*", ""))) {
				continue;
			}
			if (ent.length() < 3) {
				continue;
			}
			ent = " " + ent + " ";
			// for (String ent : GenerateNgram.generateNgramsUpto(sb.toString(),
			// 5)) {
			if (!sb.toString().contains(ent)) {
				continue;
			}
			int index = 0;
			while ((index = sb.toString().indexOf(ent, index)) >= 0) {
				if (!begins.contains(index) && index > -1) {
					int end = index + ent.length() - 1;
					int begin = index + 1;
					entIds.putIfAbsent(ent + "\t" + type, allEnts.size() + 1);
					allEnts.putIfAbsent(ent + "\t" + type, 0);
					allEnts.put(ent + "\t" + type, allEnts.get(ent + "\t" + type) + 1);
					IOUtils.write(docId + "\t" + entIds.get(ent + "\t" + type) + "\t" + 1 + "\n", entDocWriter, "UTF8");
					IOUtils.write(docId + "\t" + entIds.get(ent + "\t" + type) + "\t" + begin + "\t" + end + "\n",
							entOffsetWriter, "UTF8");
					begins.add(begin);
					index = end;
				}
				index = index + ent.length();
			}
		}
	}

	private static void writeMetadata(CSVWriter metadata, int docId, String key, Object value, String type) {

		String[] parentIdMetadata = new String[] { docId + "", key, value + "", type };
		metadata.writeNext(parentIdMetadata);
	}

	static String languageDetector(String text) throws IOException {

		// System.out.println(text);
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

	static List<String> getEntities(JSONArray person) {
		List<String> ents = new ArrayList<String>();
		if (person != null)
			for (Iterator<?> personIt = person.iterator(); personIt.hasNext();) {
				ents.add(personIt.next().toString());
			}
		return ents;
	}
	/*
	 * private static String replaceEscapeChars(String annotation) { return
	 * annotation.replace("\\", "\\\\").replace(",", "\\,").replace("\"",
	 * "\\\"]"); }
	 */

}
