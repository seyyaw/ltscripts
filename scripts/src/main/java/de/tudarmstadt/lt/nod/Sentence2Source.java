package de.tudarmstadt.lt.nod;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import org.apache.commons.io.IOUtils;

public class Sentence2Source {

	public static void main(String[] args) {
		try {
			FileOutputStream os = new FileOutputStream("/home/seid/LT/DIVID-DJ/cables/sentences_to_sources");
			for (int i = 0; i < 251288; i++) {
				IOUtils.write(i + "\t" + i + "\n", os, "UTF-8");
			}
			os.close();
		} catch (UnsupportedEncodingException e) {
			System.out.println(e.getMessage());
		} catch (IOException e) {
			System.out.println(e.getMessage());
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

}
