package de.tudarmstadt.lt.nod;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

/**
 * A preprocessor output accordingto
 * ''http://maggie.lt.informatik.tu-darmstadt.de/thesis/master/NetworksOfNames/'',
 * will produce the entity, relation,... tsv files. get the timestamp from the
 * source file and add it tot the entity file at the last column
 * 
 * @author Seid M. Yimam
 * 
 */
public class AddTimeStampToOldLeipzigEntityFile {

	public static void main(String[] args) throws IllegalArgumentException,
			IOException {
		File s2s = new File(args[0]);
		File entity = new File(args[1]);
		File source = new File(args[2]);
		addTimestampToEntity(s2s, entity, source);
	}

	// add the timestamp from the source to sentence file to the entity file
	// using the source file
	private static void addTimestampToEntity(File aS2s, File aEntity,
			File aSource) throws IllegalArgumentException, IOException {
		Map<Long, Long> s2ses = new HashMap<Long, Long>();
		LineIterator s2sIt = new LineIterator(new FileReader(aS2s));

		System.out.println("sentso pre");
		while (s2sIt.hasNext()) {
			String line = s2sIt.nextLine().trim();
			long source = Long.parseLong(line.split("\t")[0]);
			long sentence = Long.parseLong(line.split("\t")[1]);
			s2ses.put(sentence, source);
			System.out.println(sentence);
		}

		Map<Long, String> sources = new HashMap<Long, String>();
		LineIterator sourceIt = new LineIterator(new FileReader(aSource));
		System.out.println("source pre");
		while (sourceIt.hasNext()) {
			String line = sourceIt.nextLine().trim();
			long sourceId = Long.parseLong(line.split("\t")[0]);
			String date = line.split("\t")[2];// date
			sources.put(sourceId, date);
			System.out.println(sourceId);
		}

		LineIterator entityIt = new LineIterator(new FileReader(aEntity));
		FileOutputStream os = new FileOutputStream(new File(
				aEntity.getAbsolutePath() + ".timestamped"));
		System.out.println("entity pre");
		while (entityIt.hasNext()) {
			String line = entityIt.nextLine().trim();
			long entityId = Long.parseLong(line.split("\t")[0]);
			long type = Long.parseLong(line.split("\t")[1]);
			String entity = line.split("\t")[2];
			long sentId = Long.parseLong(line.split("\t")[3]);
			IOUtils.write(entityId + "\t" + type + "\t" + entity + "\t"
					+ sentId + "\t" + sources.get(s2ses.get(sentId)) + "\n",
					os, "UTF-8");

		}
		os.close();
	}
}
