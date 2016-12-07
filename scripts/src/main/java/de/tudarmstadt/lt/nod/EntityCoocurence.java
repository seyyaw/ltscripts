package de.tudarmstadt.lt.nod;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import de.tudarmstadt.lt.nod.model.Entity;
import de.tudarmstadt.lt.nod.model.Relation;

public class EntityCoocurence {
	public static final String LF = System.getProperty("line.separator");
	public static final String TAB = "\t";

	List<Entity> ents = new ArrayList<Entity>(); // All entities
	List<Relation> rels = new ArrayList<Relation>();

	
	private Map<Long, Integer> createDocEntRel(FileWriter docEntity, long dnum,
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
			for (Long ent2 : entity2FreqPerDoc.keySet()) {
				Relation rel = addRelation(EntIds.get(ent1), EntIds.get(ent2));
				relationFreqPerDoc.put(rel.getId(), Math.max(entityFreqPerDoc.get(ent1), entityFreqPerDoc.get(ent2)));
				relIds.put(rel.getId(), rel);
				/*if (relationFreqPerDoc.keySet().contains(rel.getId())) {
					relationFreqPerDoc.put(rel.getId(), relationFreqPerDoc.get(rel) + 1);
				} else {
					relationFreqPerDoc.put(rel.getId(), 1);
					relIds.put(rel.getId(), rel);
				}*/
			}
		}

		return relationFreqPerDoc;
	}

	private void writeEntDocOffsets(FileWriter entDocOffsets, int begin, int end, long dnum,
			Map<Long, Integer> entityFreqPerDoc, Map<Long, Entity> entIds, String prevNeType, StringBuffer namedEntity)
			throws IOException {
		Entity ent = addEntity(namedEntity.toString(), prevNeType.substring(2));
		if (entityFreqPerDoc.keySet().contains(ent.getId())) {
			entityFreqPerDoc.put(ent.getId(), entityFreqPerDoc.get(ent.getId()) + 1);
		} else {
			entityFreqPerDoc.put(ent.getId(), 1);
			entIds.put(ent.getId(), ent);
		}
		entDocOffsets.write(ent.getId() + TAB + begin + TAB + end + TAB + dnum + LF);
	}

	private Entity addEntity(String aNme, String aType) {
		Entity newEnt = new Entity(aNme, aType);
		if (ents.contains(newEnt)) {
			return ents.get(ents.indexOf(newEnt));
		} else {
			newEnt.setId(ents.size() + 1);
			ents.add(newEnt);
			return newEnt;
		}
	}

	private Relation addRelation(Entity ent1, Entity ent2) {
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
