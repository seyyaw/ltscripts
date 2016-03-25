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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import org.apache.commons.io.IOUtils;
import com.sree.textbytes.jtopia.Configuration;
import com.sree.textbytes.jtopia.TermDocument;
import com.sree.textbytes.jtopia.TermsExtractor;

public class ExtractKeywords {
	private static Connection conn;
	private static Statement st;

	public static void main(String[] args)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, IOException {
		Configuration.setTaggerType("default");
		Configuration.setSingleStrength(1);
		Configuration.setNoLimitStrength(1);
		Configuration.setModelFileLocation("model/default/english-lexicon.txt");
		initDB("cable", "", "", "");
		st.setFetchSize(50);
		ResultSet docSt = st.executeQuery("select * from document;");

		ThreadLocal<FileOutputStream> os = ThreadLocal.withInitial(new Supplier<FileOutputStream>() {
			@Override
			public FileOutputStream get() {
				try {
					return new FileOutputStream("important-terms-" + Thread.currentThread().getId() + ".tsv");
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}

		});

		ThreadLocal<TermsExtractor> termExtractor = ThreadLocal.withInitial(() -> new TermsExtractor());

		ExecutorService t = Executors.newFixedThreadPool(50);

		while (docSt.next()) {
			count++;
			String content = docSt.getString("content");
			Long id = docSt.getLong("id");

			t.execute(new Runnable() {
				@Override
				public void run() {

					TermDocument termDocument = termExtractor.get().extractTerms(content);
					Map<String, ArrayList<Integer>> extracted = termDocument.getFinalFilteredTerms();

					for (String term : extracted.keySet()) {
						try {
							IOUtils.write(id + "\t" + term + "\t" + extracted.get(term).get(0) + "\n", os.get(),
									"UTF-8");
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

					}

				}
			});

		}

		t.shutdown();
		while (!t.isTerminated())
			try {
				Thread.sleep(10L);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	}

	public static void initDB(String aDbName, String ip, String user, String pswd)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		String url = "jdbc:postgresql://" + ip + "/";
		String dbName = aDbName;
		String driver = "org.postgresql.Driver";
		String userName = user;
		String password = pswd;
		Class.forName(driver).newInstance();
		conn = DriverManager.getConnection(url + dbName, userName, password);
		conn.setAutoCommit(false);
		st = conn.createStatement();
	}
}
