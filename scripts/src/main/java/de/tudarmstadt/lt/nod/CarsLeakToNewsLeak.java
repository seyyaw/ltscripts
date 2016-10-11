package de.tudarmstadt.lt.nod;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.opencsv.CSVWriter;

public class CarsLeakToNewsLeak {
	public static final String LF = System.getProperty("line.separator");
	public static final String TAB = "\t";

	public static void main(String[] args) throws IOException {
		JSONParser parser = new JSONParser();
		FileWriter entity = new FileWriter(new File("entity.tsv"));
		CSVWriter writer = new CSVWriter(new FileWriter("document.csv"));

		Map<String, Integer> entIds = new HashMap<>();
		try {

			Object obj = parser.parse(new FileReader("/media/seid/DATA/LT/DIVID-DJ/cars/cars.json"));

			JSONObject jsonObject = (JSONObject) obj;
			JSONObject hitsObject = (JSONObject) jsonObject.get("hits");

			JSONArray hitsArray = (JSONArray) hitsObject.get("hits");
			int docId = 1;
			for (Iterator<?> iterator = hitsArray.iterator(); iterator.hasNext();) {
				JSONObject hitsDocument = (JSONObject) iterator.next();
				JSONObject source = (JSONObject) hitsDocument.get("_source");
				JSONObject main = (JSONObject) source.get("Main");

				String lang = (String) main.get("Language");

				if (lang.equals("de")) {

					JSONObject file = (JSONObject) source.get("File");
					String created = (String) file.get("created");
					String content = (String) main.get("Content");
					String[] entries = new String[] { docId + "", content, created };
					writer.writeNext(entries);

					JSONArray location = (JSONArray) main.get("LOCATION");
					if (location != null)
						for (Iterator<?> locIt = location.iterator(); locIt.hasNext();) {
							writeEntities(entIds, locIt.next().toString(), "LOC", docId, entity);
						}
					JSONArray org = (JSONArray) main.get("ORGANIZATION");
					if (org != null)
						for (Iterator<?> orgIt = org.iterator(); orgIt.hasNext();) {
							writeEntities(entIds, orgIt.next().toString(), "ORG", docId, entity);
						}
					JSONArray person = (JSONArray) main.get("PERSON");
					if (person != null)
						for (Iterator<?> personIt = person.iterator(); personIt.hasNext();) {
							writeEntities(entIds, personIt.next().toString(), "PER", docId, entity);
						}
					JSONArray misc = (JSONArray) main.get("MISC");
					if (misc != null)
						for (Iterator<?> miscIt = misc.iterator(); miscIt.hasNext();) {
							writeEntities(entIds, miscIt.next().toString(), "MISC", docId, entity);
						}
					docId++;
				}
			}
			writer.close();
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

	static void printKeys(JSONObject obeject) {
		for (Iterator<?> iterator = obeject.keySet().iterator(); iterator.hasNext();) {
			String key = (String) iterator.next();
			System.out.println(key);
		}
	}

	static void writeEntities(Map<String, Integer> entIds, String namedEntity, String type, int dn, FileWriter entity)
			throws IOException {
		entIds.putIfAbsent(namedEntity.toLowerCase() + type, entIds.size() + 1);
		int id = entIds.get(namedEntity.toLowerCase() + type);
		entity.write(id + TAB + namedEntity + TAB + type + TAB + dn + LF);
	}
}
