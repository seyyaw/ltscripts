package de.tudarmstadt.lt.nod;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

public class RemoveNoNELines {

	public static void main(String[] args) throws IOException {

		// romoveNoNE(args);
		// romoveWronEventTime(args);
		removeduplicatedenitityDocs(args);
	}

	private static void romoveNoNE(String[] args) throws IOException, FileNotFoundException {
		LineIterator it = FileUtils.lineIterator(new File(args[0]));

		OutputStream out = new FileOutputStream(new File(args[0] + ".clean"));
		while (it.hasNext()) {
			String line = it.nextLine();
			if (line.split("\t").length > 1) {
				IOUtils.write(line + "\n", out, "UTF8");
			}
		}
		out.close();
	}

	private static void romoveWronEventTime(String[] args) throws IOException, FileNotFoundException {
		LineIterator it = FileUtils.lineIterator(new File(args[0]));

		OutputStream out = new FileOutputStream(new File(args[0] + ".clean"));
		while (it.hasNext()) {
			String line = it.nextLine();
			if (line.split("\t").length > 1 && !line.split("\t")[1].isEmpty() && !line.split("\t")[2].isEmpty()) {
				IOUtils.write(line + "\n", out, "UTF8");
			}
		}
		out.close();
	}

	private static void removeduplicatedenitityDocs(String[] args) throws IOException, FileNotFoundException {
		LineIterator it = FileUtils.lineIterator(new File(args[0]));

		Set<String> dups = new HashSet<>();
		OutputStream out = new FileOutputStream(new File(args[0] + ".clean"));
		while (it.hasNext()) {
			String line = it.nextLine();
			String id = line.split("\t")[0] + "-" + line.split("\t")[1];
			if (!dups.contains(id)) {
				IOUtils.write(line + "\n", out, "UTF8");
				dups.add(id);
			}

		}
		out.close();
	}
}
