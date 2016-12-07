package de.tudarmstadt.lt.nod;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

public class PSQLCopy {

	private static Connection conn;
	private static Connection rootcnn;
	static Statement cdbStatement;
	static Statement st;

	static String dbName;
	static String dbUser;
	static String dbPass;
	static String dbUrl;

	public static void main(String[] args)
			throws IOException, SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		int batchSize = 1000;
		getConfigs("newsleak.properties");
		try {
			// dbName is the new db tp be created
			createDB(dbName, dbUrl, dbUser, dbPass);
			System.out.println("DB created");
		} catch (SQLException e) {
			System.out.println(e);
			// Check if DB exists
		}

		initDB(dbName, dbUrl, dbUser, dbPass);
		// initDBCreate(args[0], args[1], args[2], args[3]);

		String createScripts = FileUtils.readFileToString(new File("createtable.sql")).replace("\n", "");
		// StringTokenizer st = new StringTokenizer(createScripts, ";");
		try {
			st.executeUpdate(createScripts.toString());
			System.out.println("Schema created");
		} catch (Exception e) {
			System.out.println(e);
		}

		File document = new File("document.csv");
		File metadata = new File("metadata.csv");

		File entity = new File("cooc_entities/part-00000");
		File documententity = new File("cooc_doc_entities/part-00000");
		// File relationship = new File("cooc_relationships/part-00000");
		// File documentrelationship = new
		// File("cooc_doc_relationships/part-00000");
		File entityoffset = new File("coocdoc_entity_offset/part-00000");

		//File evventtime = new File("result/heideltimex_date.tsv");

		CopyManager cpManager = ((PGConnection) conn).getCopyAPI();
		// ======Documwents and Metadata copying ====

		System.out.println("Importing documents");
		st.executeUpdate("TRUNCATE TABLE document CASCADE;");
		cpManager.copyIn("COPY document FROM STDIN WITH CSV", new FileReader(document));

		System.out.println("Importing metadata");
		st.executeUpdate("TRUNCATE TABLE metadata;");
		cpManager.copyIn("COPY metadata FROM STDIN WITH CSV", new FileReader(metadata));

		// ======Coocurences ====

		System.out.println("Importing entities");
		st.executeUpdate("TRUNCATE TABLE entity;");
		cpManager.copyIn("COPY entity FROM STDIN with delimiter E'\t'", new FileReader(entity));

		System.out.println("Importing documententities");
		st.executeUpdate("TRUNCATE TABLE documententity;");
		cpManager.copyIn("COPY documententity FROM STDIN with delimiter E'\t'", new FileReader(documententity));

		System.out.println("Importing entityoffsets");
		st.executeUpdate("TRUNCATE TABLE entityoffset;");
		cpManager.copyIn("COPY entityoffset FROM STDIN with delimiter E'\t'", new FileReader(entityoffset));

		// ======Event Expression ====
		System.out.println("Importing Event expresions");
		st.executeUpdate("TRUNCATE TABLE eventtime;");
	//	cpManager.copyIn("COPY eventtime FROM STDIN with delimiter E'\t'", new FileReader(evventtime));

		// We do not need \r in the document
		String removeCR = "UPDATE document SET content = regexp_replace(content, E'\\r', '', 'g' );" ;
		st.executeUpdate(removeCR);
		
		// Create Indices
		System.out.println("Creating indices");
		createScripts = FileUtils.readFileToString(new File("createindex.sql")).replace("\n", "");
		// copySlow(batchSize, entity, documententity, entityoffset, cpManager);

		try {
			st.executeUpdate(createScripts.toString());
			System.out.println("Index created");
		} catch (Exception e) {
			System.out.println(e);
		}

		conn.close();

	}

	private static void copySlow(int batchSize, File entity, File documententity, File entityoffset,
			CopyManager cpManager) throws FileNotFoundException, SQLException, IOException {
		System.out.println("Importing Entities");
		LineIterator entIt = IOUtils.lineIterator(new FileReader(entity));
		StringBuffer sb = new StringBuffer();
		int i = 0;
		while (entIt.hasNext()) {
			sb.append(entIt.nextLine() + "\n");

			if (i % batchSize == 0) {
				cpManager.copyIn("COPY entity FROM STDIN with delimiter E'\t'", new StringReader(sb.toString()));
				sb = new StringBuffer();
			}
		}
		cpManager.copyIn("COPY entity FROM STDIN with delimiter E'\t'", new StringReader(sb.toString()));

		System.out.println("Importing entity documents");
		sb = new StringBuffer();
		i = 0;

		LineIterator dcIt = IOUtils.lineIterator(new FileReader(documententity));

		while (dcIt.hasNext()) {
			sb.append(dcIt.nextLine() + "\n");

			if (i % batchSize == 0) {
				cpManager.copyIn("COPY documententity FROM STDIN with delimiter E'\t'",
						new StringReader(sb.toString()));
				sb = new StringBuffer();
			}
		}
		cpManager.copyIn("COPY documententity FROM STDIN with delimiter E'\t'", new StringReader(sb.toString()));

		System.out.println("Importing entity offsets");
		LineIterator entoffIt = IOUtils.lineIterator(new FileReader(entityoffset));

		sb = new StringBuffer();
		i = 0;
		while (entoffIt.hasNext()) {
			sb.append(entoffIt.nextLine() + "\n");

			if (i % batchSize == 0) {
				cpManager.copyIn("COPY entityoffset FROM STDIN with delimiter E'\t'", new StringReader(sb.toString()));
				sb = new StringBuffer();
			}
		}
		cpManager.copyIn("COPY entityoffset FROM STDIN with delimiter E'\t'", new StringReader(sb.toString()));
	}

	public static void initDBCreate(String aDbName, String ip, String user, String pswd)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		String url = "jdbc:postgresql://" + ip + "/";
		String dbName = aDbName;
		String driver = "org.postgresql.Driver";
		String userName = user;
		String password = pswd;
		Class.forName(driver).newInstance();
		conn = DriverManager.getConnection(url + dbName, userName, password);
		st = conn.createStatement();
		// conn.setAutoCommit(false);
	}

	public static void initDB(String aDbName, String address, String user, String pass)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		String url = "jdbc:postgresql://" + address + "/";
		String dbName = aDbName;
		String driver = "org.postgresql.Driver";
		String userName = user;
		String password = pass;
		Class.forName(driver).newInstance();
		conn = DriverManager.getConnection(url + dbName, userName, password);
		st = conn.createStatement();
		// conn.setAutoCommit(false);
	}

	static void createDB(String newDBNAme, String address, String user, String pass) throws SQLException {

		rootcnn = DriverManager.getConnection("jdbc:postgresql://" + address, user, pass);
		cdbStatement = rootcnn.createStatement();

		// cdbStatement.executeUpdate("DROP DATABASE IF EXISTS "+
		// newDBNAme+";");

		cdbStatement.executeUpdate("CREATE DATABASE " + newDBNAme
				+ " WITH ENCODING='UTF8' LC_CTYPE='en_US.UTF-8' LC_COLLATE='en_US.UTF-8' TEMPLATE=template0 CONNECTION LIMIT=-1; GRANT ALL ON DATABASE "
				+ newDBNAme + " TO " + user + ";");
	}

	static void getConfigs(String config) throws IOException {
		Properties prop = new Properties();
		InputStream input = null;

		input = new FileInputStream(config);

		prop.load(input);

		dbName = prop.getProperty("dbname");
		dbUser = prop.getProperty("dbuser");
		dbUrl = prop.getProperty("dbaddress");
		dbPass = prop.getProperty("dbpass");

		if (input != null) {
			try {
				input.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}
}
