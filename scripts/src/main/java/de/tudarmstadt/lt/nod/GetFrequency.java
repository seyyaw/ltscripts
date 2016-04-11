package de.tudarmstadt.lt.nod;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

public class GetFrequency {

	private static Connection conn;
	private static Statement st;

	public static void main(String[] args)
			throws IOException, SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		FileOutputStream osentity = new FileOutputStream("entities_withfreq.tsv");
		LineIterator itent = FileUtils.lineIterator(new File("entities.tsv"));
		
		initDB("enron", "", "", "");
		st.setFetchSize(50);
		while (itent.hasNext()) {
			String line = itent.nextLine();
			int id = Integer.valueOf(line.split("\t")[0]);		
			ResultSet docEntSt = st
					.executeQuery("SELECT SUM(frequency) as f FROM documententity WHERE entityid = " + id + ";");
			int total = docEntSt.next() ? docEntSt.getInt("f") : 0;
			IOUtils.write(line + "\t" + total + "\n", osentity, "UTF-8");
			if (id % 1000 == 0) {
				System.out.println(id);
			}
		}
		System.out.println("Entity Done");
		osentity.close();

		LineIterator itrel = FileUtils.lineIterator(new File("relationships.tsv"));
		FileOutputStream osrel = new FileOutputStream("relationships_withfreq.tsv");
		while (itrel.hasNext()) {
			String line = itrel.nextLine();
			int id = Integer.valueOf(line.split("\t")[0]);
			ResultSet docEntSt = st
					.executeQuery("SELECT SUM(frequency) as f FROM documentrelationship WHERE relid = " + id + ";");
			int total = docEntSt.next() ? docEntSt.getInt("f") : 0;
			IOUtils.write(line + "\t" + total + "\n", osrel, "UTF-8");
			if (id % 1000 == 0) {
				System.out.println(id);
			}
		}
		System.out.println("rel Done");
		osrel.close();

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
