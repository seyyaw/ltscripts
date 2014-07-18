package de.tudarmstadt.lt.nod;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class LeipzigDayDataRetriever {
	private static final String EXTRACTED_FOLDER = "/extracted";
	private static final String ENCODING = "UTF-8";
	private static final String DEFAULT_URL = "http://asvdoku.informatik.uni-leipzig.de/wdt/";
	private static final Logger LOG = Logger
			.getLogger(LeipzigDayDataRetriever.class.getName());

	public static void main(String[] args) throws IOException,
			CompressorException {
		String leipizigDayDataUrl;
		if (args.length == 0) {
			LOG.info("USAGE 'java -jar scripts.jar url dir' WHERE url is the base url to get the "
					+ "Leipzig day corpora and dir is the directory where you like to store the "
					+ "processed data");
			leipizigDayDataUrl = DEFAULT_URL;
		} else {
			leipizigDayDataUrl = args[0];
		}

		Document doc = Jsoup.connect(leipizigDayDataUrl).get();
		Set<String> fileNames = new HashSet<String>();
		for (Element fileName : doc.select("td a")) {
			if (fileName.attr("href").endsWith(".bz2")) {
				fileNames.add(fileName.attr("href"));
			}
		}
		String path = "./LeipzigDailyData/";
		for (String fileName : fileNames) {
			if (!new File(path).exists()) {
				FileUtils.forceMkdir(new File(path));
			}
			File file = new File(path + fileName);
			if (!file.exists()) {

				downloadFile(leipizigDayDataUrl + fileName,
						file.getAbsolutePath());
				LOG.info(fileName + " is downloaded");
				extractZip(file);
				LOG.info(fileName + " is extracted");
			}
		}

	}

	/**
	 * Downloads a .bz2 File from a given url and save the zip file to a file
	 * system
	 * 
	 * @param aUrl
	 *            The full url path of the file to be download
	 * @param aPath
	 *            Directory where the filew will be saved
	 * @param aFileName
	 *            The name of the file to save to
	 */

	public static void downloadFile(String aUrl, String aFileName) {
		try {
			URL downloadUrl = new URL(aUrl);
			URLConnection conn = downloadUrl.openConnection();
			InputStream in = conn.getInputStream();
			FileOutputStream out = new FileOutputStream(aFileName);
			byte[] b = new byte[1024];
			int count;
			while ((count = in.read(b)) >= 0) {
				out.write(b, 0, count);
			}
			out.flush();
			out.close();
			in.close();

		} catch (IOException e) {
			LOG.error(e.getCause());
		}
	}

	public static void extractZip(File aFile) throws CompressorException,
			IOException {
		BZip2CompressorInputStream input = new BZip2CompressorInputStream(
				new FileInputStream(aFile));
		BufferedReader br = new BufferedReader(new InputStreamReader(input));
		File extractedFolder = new File(aFile.getParent() + EXTRACTED_FOLDER);
		if (!extractedFolder.exists())
			FileUtils.forceMkdir(extractedFolder);
		String fileName = FilenameUtils.getBaseName(aFile.getName());
		File extractedFile = new File(extractedFolder, fileName);

		processAndWriteTsvFiles(br, fileName, extractedFile);
	}

	private static void processAndWriteTsvFiles(BufferedReader br,
			String fileName, File extractedFile) throws IOException,
			FileNotFoundException {
		String line = null;
		StringBuilder sentences = new StringBuilder();
		Integer sentenceId = 1;
		String reformattedDate = fileName.replaceFirst(
				"(\\d{4})(\\d{2})(\\d{2})", "$1-$2-$3");
		// source,sourceId
		Map<String, Integer> sources = new LinkedHashMap<String, Integer>();
		// sourceId,sentenceID
		Map<Integer, Integer> sentenceSources = new LinkedHashMap<Integer, Integer>();
		while ((line = br.readLine()) != null) {
			StringTokenizer lineSt = new StringTokenizer(line, "\t");
			sentences.append(sentenceId + "\t" + lineSt.nextToken() + "\t"
					+ reformattedDate + "\n");
			String source = lineSt.nextToken();
			if (sources.get(source) == null) {
				sources.put(source, sources.size() + 1);
			}
			sentenceSources.put(sentenceId, sources.get(source));
			sentenceId++;
		}
		int size = (int) Math.ceil((double) sentenceSources.size() / 1000000.0);
		String corpusName = extractedFile.getParent() + "/dail_news_"
				+ extractedFile.getName();
		OutputStream sentenceOs = new FileOutputStream(corpusName + "_" + size
				+ "M-sentences.txt");
		OutputStream sourceOs = new FileOutputStream(corpusName + "_" + size
				+ "M-sources.txt");
		OutputStream invSoOs = new FileOutputStream(corpusName + "_" + size
				+ "M-inv_so.txt");

		IOUtils.write(sentences.toString(), sentenceOs, ENCODING);

		for (String source : sources.keySet()) {

			IOUtils.write(sources.get(source) + "\t" + source + "\t"
					+ reformattedDate + "\n", sourceOs, ENCODING);
		}

		for (int i : sentenceSources.keySet()) {
			IOUtils.write(sentenceSources.get(i) + "\t" + i + "\n", invSoOs,
					ENCODING);
		}
	}
}
