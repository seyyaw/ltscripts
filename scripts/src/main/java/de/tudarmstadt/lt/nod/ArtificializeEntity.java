package de.tudarmstadt.lt.nod;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ArtificializeEntity {
	private static Connection conn;
	private static Statement st;

	public static void main(String[] args)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		initDB("cable", "192.168.164.227:5433", "newsreader", "newsreader");
		st.setFetchSize(50);

		addArtificialTypes();
	}

	private static void addArtificialTypes() throws SQLException {
		ResultSet entTypes = st.executeQuery("select distinct type from entity;");
		while (entTypes.next()) {
			String type = entTypes.getString("type");
			ResultSet entsPerType = conn.createStatement()
					.executeQuery("select * from entity where type =\'" + type + "\';");
			int i = 1;
			PreparedStatement ps = conn.prepareStatement("UPDATE entity set type = ? WHERE id = ?;");
			while (entsPerType.next()) {
				i++;
				long id = entsPerType.getLong("id");
				if (i % 2 == 0) {					
					ps.setString(1, "Art" + entsPerType.getString("type"));
					ps.setLong(2, id);
					ps.addBatch();
					/*ps.executeUpdate();
					ps.close();*/
				}
				if (i % 100 == 0) {
					System.out.println(i);
				}
			}
			ps.executeBatch();
			conn.commit();
			ps.close();
			System.out.println(type + " iscompleted");
		}
	}

	static void initDB(String dbName, String ip, String user, String pswd)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {

		String url = "jdbc:postgresql://" + ip + "/";
		String driver = "org.postgresql.Driver";
		String userName = user;
		String password = pswd;
		Class.forName(driver).newInstance();
		conn = DriverManager.getConnection(url + dbName, userName, password);
		conn.setAutoCommit(false);
		st = conn.createStatement();
	}
}
