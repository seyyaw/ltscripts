package de.tudarmstadt.lt.nod;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.opencsv.CSVWriter;

public class CarsLeakToNewsLeakOlDentitiesFormat {
	public static final String LF = System.getProperty("line.separator");
	public static final String TAB = "\t";
	static Set<String> entdoc = new HashSet<>();

	public static void main(String[] args) throws IOException {
		JSONParser parser = new JSONParser();
		OutputStream entity = new FileOutputStream (new File("entity.tsv"));
		CSVWriter metadata = new CSVWriter(new FileWriter("metadata.csv"));
		CSVWriter writer = new CSVWriter(new FileWriter("document.csv"));
		OutputStream entityoffset = new FileOutputStream(new File("entityoffset.tsv"));
		OutputStream entityDocument =  new FileOutputStream(new File("entityDocument.tsv"));
		Map<String, Integer> entIds = new HashMap<>();

		try {

			Object obj = parser.parse(new FileReader("cars.json"));

			JSONObject jsonObject = (JSONObject) obj;
			JSONObject hitsObject = (JSONObject) jsonObject.get("hits");

			JSONArray hitsArray = (JSONArray) hitsObject.get("hits");
			int docId = 1;
			boolean doubled = true;
			int created = 1950;
			for (Iterator<?> iterator = hitsArray.iterator(); iterator.hasNext();) {
				JSONObject hitsDocument = (JSONObject) iterator.next();
				JSONObject source = (JSONObject) hitsDocument.get("_source");
				JSONObject main = (JSONObject) source.get("Main");

				List<String> ents = new ArrayList<>();

				JSONObject file = (JSONObject) source.get("File");
				// String created = (String) file.get("created");

				String content = ((String) main.get("Content")).replace("\n", "<br>");
				String[] entries = new String[] { docId + "", content, created + "-01-01" };
				Set<Integer> begins = new HashSet<>();
				JSONArray person = (JSONArray) main.get("PERSON");
				int index = 0;
				int reduce = 1;
				if (person != null)
					for (Iterator<?> personIt = person.iterator(); personIt.hasNext();) {
						String ent = personIt.next().toString();

						if (!StringUtils.isAlphanumeric(ent.replace(" ", "").replace("-", "").replace(";", "")
								.replace(".", "").replace("&", "").replace("*", ""))) {
							continue;
						}
						index = content.indexOf(ent, index);
						entIds.putIfAbsent(ent.toLowerCase() + "PER", entIds.size() + 1);
						int id = entIds.get(ent.toLowerCase() + "PER");
						entdoc.add(docId + "-" + id);
						int count = 1;
						if (!begins.contains(index) && index > -1) {
							// addEntities(ent, "PER", docId, ents, index, index
							// + ent.length());
							writeEntities(entIds, ent, "PER", docId, id, entityDocument, entityoffset, index,
									index + ent.length());
							begins.add(index);
						}

						while (index >= 0) {
							index = content.indexOf(ent, index + 1);
							if (!begins.contains(index) && index > -1) {
								// addEntities(ent, "PER", docId, ents, index,
								// index + ent.length());
								writeEntities(entIds, ent, "PER", docId, id, entityDocument, entityoffset, index,
										index + ent.length());

								begins.add(index);
								count++;
							}
							reduce++;
						}
						IOUtils.write(id + TAB + ent + TAB + "PER" + TAB + count + TAB + "false" + LF, entity, "UTF8");
						index = 0;
					}

				JSONArray location = (JSONArray) main.get("LOCATION");

				if (location != null) {
					for (Iterator<?> locIt = location.iterator(); locIt.hasNext();) {
						String ent = locIt.next().toString();
						if (!StringUtils.isAlphanumeric(ent.replace(" ", "").replace("-", "").replace(";", "")
								.replace(".", "").replace("&", "").replace("*", ""))) {
							continue;
						}
						index = content.indexOf(ent, index);
						entIds.putIfAbsent(ent.toLowerCase() + "PER", entIds.size() + 1);
						int id = entIds.get(ent.toLowerCase() + "PER");
						entdoc.add(docId + "-" + id);
						int count = 1;
						if (!begins.contains(index) && index > -1) {
							// addEntities(ent, "LOC", docId, ents, index, index
							// + ent.length());
							writeEntities(entIds, ent, "LOC", docId, id, entityDocument, entityoffset, index,
									index + ent.length());
							begins.add(index);
						}
						while (index >= 0) {
							index = content.indexOf(ent, index + 1);
							if (!begins.contains(index) && index > -1) {
								// addEntities(ent, "LOC", docId, ents, index,
								// index + ent.length());
								writeEntities(entIds, ent, "LOC", docId, id, entityDocument, entityoffset, index,
										index + ent.length());
								begins.add(index);
								count++;
							}
							reduce++;
						}
						IOUtils.write(id + TAB + ent + TAB + "LOC" + TAB + count + TAB + "false" + LF, entity, "UTF8");
						index = 0;
					}
				}
				JSONArray org = (JSONArray) main.get("ORGANIZATION");
				if (org != null)
					for (Iterator<?> orgIt = org.iterator(); orgIt.hasNext();) {
						String ent = orgIt.next().toString();
						if (!StringUtils.isAlphanumeric(ent.replace(" ", "").replace("-", "").replace(";", "")
								.replace(".", "").replace("&", "").replace("*", ""))) {
							continue;
						}

						index = content.indexOf(ent, index);
						entIds.putIfAbsent(ent.toLowerCase() + "PER", entIds.size() + 1);
						int id = entIds.get(ent.toLowerCase() + "PER");
						entdoc.add(docId + "-" + id);
						int count = 1;
						if (!begins.contains(index) && index > -1) {
							// addEntities(ent, "ORG", docId, ents, index, index
							// + ent.length());
							writeEntities(entIds, ent, "ORG", docId, id, entityDocument, entityoffset, index,
									index + ent.length());

							begins.add(index);
						}
						while (index >= 0) {
							index = content.indexOf(ent, index + 1);
							if (!begins.contains(index) && index > -1) {
								// addEntities(ent, "ORG", docId, ents, index,
								// index + ent.length());
								writeEntities(entIds, ent, "ORG", docId, id, entityDocument, entityoffset, index,
										index + ent.length());
								begins.add(index);
								count++;
							}
							reduce++;
						}
						IOUtils.write(id + TAB + ent + TAB + "ORG" + TAB + count + TAB + "false" + LF, entity, "UTF8");

						index = 0;
					}
				index = 0;
				JSONArray misc = (JSONArray) main.get("MISC");
				if (misc != null)
					for (Iterator<?> miscIt = misc.iterator(); miscIt.hasNext();) {
						String ent = miscIt.next().toString();
						if (!StringUtils.isAlphanumeric(ent.replace(" ", "").replace("-", "").replace(";", "")
								.replace(".", "").replace("&", "").replace("*", ""))) {
							continue;
						}
						index = content.indexOf(ent, index);
						entIds.putIfAbsent(ent.toLowerCase() + "PER", entIds.size() + 1);
						int id = entIds.get(ent.toLowerCase() + "PER");
						entdoc.add(docId + "-" + id);
						int count = 1;
						if (!begins.contains(index) && index > -1) {
							// addEntities(ent, "MISC", docId, ents, index,
							// index + ent.length());
							writeEntities(entIds, ent, "MISC", docId, id, entityDocument, entityoffset, index,
									index + ent.length());

							begins.add(index);
						}
						while (index >= 0) {
							index = content.indexOf(ent, index + 1);
							if (!begins.contains(index) && index > -1) {
								// addEntities(ent, "MISC", docId, ents, index,
								// index + ent.length());
								writeEntities(entIds, ent, "MISC", docId, id, entityDocument, entityoffset, index,
										index + ent.length());
								begins.add(index);
								count++;
							}
							reduce++;
						}
						IOUtils.write(id + TAB + ent + TAB + "MISC" + TAB + count + TAB + "false" + LF, entity, "UTF8");

						index = 0;
					}

			//	if (!ents.isEmpty()) {
					writer.writeNext(entries);
					//entity.write(docId + TAB + StringUtils.join(ents, "%,%") + LF);

					String filename = (String) file.get("filename");
					String uuid = (String) file.get("uuid");
					String[] fileMetadata = new String[] { docId + "", "Filename", filename, "Text" };
					metadata.writeNext(fileMetadata);
					String[] uuidMetadata = new String[] { docId + "", "Uuid", uuid, "Text" };
					metadata.writeNext(uuidMetadata);
			//	}
				docId++;
				if (!doubled) {
					created = created + 1;
					doubled = true;
				} else {
					doubled = false;
				}

			}

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
		} finally {
			writer.close();
			metadata.close();
			entity.close();
		}
	}

	static void printKeys(JSONObject obeject) {
		for (Iterator<?> iterator = obeject.keySet().iterator(); iterator.hasNext();) {
			String key = (String) iterator.next();
			System.out.println(key);
		}
	}

	static void addEntities(String namedEntity, String type, int dn, List<String> entity, int begin, int end)
			throws IOException {

		if (!StringUtils.isAlphanumeric(namedEntity.replace(" ", "").replace("-", "").replace(";", "").replace(".", "")
				.replace("&", "").replace("*", ""))) {
			System.out.println(namedEntity);
		} else

			entity.add(namedEntity + "%#%" + type + "%#%" + begin + "%#%" + end);
	}

	static void writeEntities(Map<String, Integer> entIds, String namedEntity, String type, int dn, int id,
			OutputStream docEntity, OutputStream entoffset, int begin, int end) throws IOException {
		IOUtils.write(dn + TAB + id + TAB + begin + TAB + end + LF, entoffset,"UTF8");
		IOUtils.write(dn + TAB + id + TAB + 1 + LF, docEntity, "UTF8");

	}
}
