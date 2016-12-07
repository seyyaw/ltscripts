package de.tudarmstadt.lt.nod;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.opencsv.CSVWriter;

import de.tudarmstadt.lt.nod.model.Entity;
import de.tudarmstadt.lt.nod.model.Relation;

public class CarsLeakToNewsLeakwithRelations {
	public static final String LF = System.getProperty("line.separator");
	public static final String TAB = "\t";

	static List<Entity> ents = new ArrayList<Entity>(); // All entities
	static List<Relation> rels = new ArrayList<Relation>();

	public static void main(String[] args) throws IOException {
		JSONParser parser = new JSONParser();
		FileWriter entity = new FileWriter(new File("entity.tsv"));
		CSVWriter writer = new CSVWriter(new FileWriter("document.csv"));

		FileWriter entDocOffsets = new FileWriter(new File("entDocOffsets.tsv"));
		FileWriter docEntity = new FileWriter(new File("docEntity.tsv"));

		FileWriter documentRelationship = new FileWriter(new File("documentRelationship.tsv"));

		Map<Long, Entity> entIds = new HashMap<>();

		try {

			Object obj = parser.parse(new FileReader("cars.json"));

			JSONObject jsonObject = (JSONObject) obj;
			JSONObject hitsObject = (JSONObject) jsonObject.get("hits");

			JSONArray hitsArray = (JSONArray) hitsObject.get("hits");
			int docId = 1;
			for (Iterator<?> iterator = hitsArray.iterator(); iterator.hasNext();) {
				NavigableMap<Long, Integer> entityFreqPerDoc = new TreeMap<>();
				JSONObject hitsDocument = (JSONObject) iterator.next();
				JSONObject source = (JSONObject) hitsDocument.get("_source");
				JSONObject main = (JSONObject) source.get("Main");

				String lang = (String) main.get("Language");

				JSONObject file = (JSONObject) source.get("File");
				String created = (String) file.get("created");
				String content = (String) main.get("Content");
				String[] entries = new String[] { docId + "", content, created };
				writer.writeNext(entries);

				JSONArray location = (JSONArray) main.get("LOCATION");
				if (location != null)
					for (Iterator<?> locIt = location.iterator(); locIt.hasNext();) {
						writeEntDocOffsets(entDocOffsets, 0, 0, docId, entityFreqPerDoc, entIds, "LOC",
								locIt.next().toString());
					}
				JSONArray org = (JSONArray) main.get("ORGANIZATION");
				if (org != null)
					for (Iterator<?> orgIt = org.iterator(); orgIt.hasNext();) {
						writeEntDocOffsets(entDocOffsets, 0, 0, docId, entityFreqPerDoc, entIds, "ORG",
								orgIt.next().toString());
					}
				JSONArray person = (JSONArray) main.get("PERSON");
				if (person != null)
					for (Iterator<?> personIt = person.iterator(); personIt.hasNext();) {
						writeEntDocOffsets(entDocOffsets, 0, 0, docId, entityFreqPerDoc, entIds, "PER",
								personIt.next().toString());
					}
				JSONArray misc = (JSONArray) main.get("MISC");
				if (misc != null)
					for (Iterator<?> miscIt = misc.iterator(); miscIt.hasNext();) {
						writeEntDocOffsets(entDocOffsets, 0, 0, docId, entityFreqPerDoc, entIds, "MISC",
								miscIt.next().toString());
					}

				System.out.println(entityFreqPerDoc.size());
				if (entityFreqPerDoc.size() > 0) {
					Map<Long, Relation> relIds = new HashMap<>();
					Map<Long, Integer> relationFreqPerDoc = createDocEntRel(docEntity, docId, entityFreqPerDoc, entIds,
							relIds);
					for (long rel : relationFreqPerDoc.keySet()) {
						documentRelationship.write(docId + TAB + rel + TAB + relationFreqPerDoc.get(rel) + LF);
						relIds.get(rel).setFrequency(relIds.get(rel).getFrequency() + relationFreqPerDoc.get(rel));
					}
				}
				docId++;
				System.out.println("doc=" + docId);
			}

			writer.close();
			entity.close();
			docEntity.close();
			entDocOffsets.close();
			documentRelationship.close();
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

	/*
	 * static void writeEntities(Map<String, Integer> entIds, String
	 * namedEntity, String type, int dn, NavigableMap<Long, Integer>
	 * entityFreqPerDoc, FileWriter entity, FileWriter entDocOffsets) throws
	 * IOException { entIds.putIfAbsent(namedEntity.toLowerCase() + type,
	 * entIds.size() + 1); int id = entIds.get(namedEntity.toLowerCase() +
	 * type); entity.write(id + TAB + namedEntity + TAB + type + TAB + 0 + TAB +
	 * 0 + TAB + dn + LF);
	 * 
	 * writeEntDocOffsets(entDocOffsets, 0, 0, dn, entityFreqPerDoc, entIds,
	 * type, namedEntity); }
	 */
	private static Map<Long, Integer> createDocEntRel(FileWriter docEntity, long dnum,
			NavigableMap<Long, Integer> entityFreqPerDoc, Map<Long, Entity> EntIds, Map<Long, Relation> relIds)
			throws IOException {

		for (Long ent : entityFreqPerDoc.keySet()) {
			docEntity.write(dnum + TAB + ent + TAB + entityFreqPerDoc.get(ent) + LF);
			EntIds.get(ent).setFrequency(EntIds.get(ent).getFrequency() + entityFreqPerDoc.get(ent));
		}

		Map<Long, Integer> relationFreqPerDoc = new HashMap<>();

		Long lastEnt = entityFreqPerDoc.lastKey();
		for (Long ent1 : entityFreqPerDoc.keySet()) {

			Map<Long, Integer> entity2FreqPerDoc = entityFreqPerDoc.subMap(ent1, false, lastEnt, true);
			System.out.println(entity2FreqPerDoc.size());
			for (Long ent2 : entityFreqPerDoc.keySet()) {
				Relation rel = addRelation(EntIds.get(ent1), EntIds.get(ent2));
				relationFreqPerDoc.put(rel.getId(), Math.max(entityFreqPerDoc.get(ent1), entityFreqPerDoc.get(ent2)));
				relIds.put(rel.getId(), rel);

				if (relationFreqPerDoc.keySet().contains(rel.getId())) {
					relationFreqPerDoc.put(rel.getId(), relationFreqPerDoc.get(rel) + 1);
				} else {
					relationFreqPerDoc.put(rel.getId(), 1);
					relIds.put(rel.getId(), rel);
				}

			}
		}

		return relationFreqPerDoc;
	}

	private static void writeEntDocOffsets(FileWriter entDocOffsets, int begin, int end, int dnum,
			Map<Long, Integer> entityFreqPerDoc, Map<Long, Entity> entIds, String prevNeType, String namedEntity)
			throws IOException {
		Entity ent = addEntity(namedEntity, prevNeType.substring(2));
		if (entityFreqPerDoc.keySet().contains(ent.getId())) {
			entityFreqPerDoc.put(ent.getId(), entityFreqPerDoc.get(ent.getId()) + 1);
		} else {
			entityFreqPerDoc.put(ent.getId(), 1);
			entIds.put(ent.getId(), ent);
		}
		entDocOffsets.write(ent.getId() + TAB + begin + TAB + end + TAB + dnum + LF);
	}

	private static Entity addEntity(String aNme, String aType) {
		Entity newEnt = new Entity(aNme, aType);
		if (ents.contains(newEnt)) {
			return ents.get(ents.indexOf(newEnt));
		} else {
			newEnt.setId(ents.size() + 1);
			ents.add(newEnt);
			return newEnt;
		}
	}

	private static Relation addRelation(Entity ent1, Entity ent2) {
		Relation newRel = new Relation(ent1, ent2);
		Relation newRelRev = new Relation(ent2, ent1);
		if (rels.contains(newRel)) {
			return rels.get(rels.indexOf(newRel));
		} else if (rels.contains(newRelRev)) {
			return rels.get(rels.indexOf(newRel));
		} else {
			newRel.setId(rels.size() + 1);
			rels.add(newRel);
			return newRel;
		}
	}
}
