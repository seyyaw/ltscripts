package de.tudarmstadt.lt.nod;

import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import com.sree.textbytes.jtopia.Configuration;
import com.sree.textbytes.jtopia.TermDocument;
import com.sree.textbytes.jtopia.TermsExtractor;

public class ExtractKeywords {
	private static Connection conn;
	private static Statement st;

	public static void main(String[] args) throws InstantiationException, IllegalAccessException,
			ClassNotFoundException, SQLException, IOException {
		Configuration.setTaggerType("default");
		Configuration.setSingleStrength(1);
		Configuration.setNoLimitStrength(1);
		Configuration.setModelFileLocation("model/default/english-lexicon.txt");
		TermsExtractor termExtractor = new TermsExtractor();
		TermDocument termDocument = new TermDocument();
		initDB("cable");
		st.setFetchSize(50);
		ResultSet docSt = st.executeQuery("select * from document;");

		FileOutputStream os = new FileOutputStream("important-terms.tsv");
		while (docSt.next()) {
			String content = docSt.getString("content");
			Long id = docSt.getLong("id");
			termDocument = termExtractor.extractTerms(content);
			Map<String, ArrayList<Integer>> extracted = termDocument.getFinalFilteredTerms();
			for(String term:extracted.keySet()){				
			IOUtils.write(id + "\t" + term +"\t" + extracted.get(term).get(0) + "\n", os, "UTF-8");
			}
		}

	}

	public static void initDB(String aDbName)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		String url = "jdbc:postgresql://130.83.164.196/";
		String dbName = aDbName;
		String driver = "org.postgresql.Driver";
		String userName = "seid";
		String password = "seid";
		Class.forName(driver).newInstance();
		conn = DriverManager.getConnection(url + dbName, userName, password);
		conn.setAutoCommit(false);
		st = conn.createStatement();
	}
}
