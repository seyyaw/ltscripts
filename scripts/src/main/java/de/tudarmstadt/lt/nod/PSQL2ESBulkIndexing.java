package de.tudarmstadt.lt.nod;

import java.io.FileWriter;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;

import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.elasticsearch.node.NodeBuilder.*;

import com.google.gson.stream.JsonWriter;

public class PSQL2ESBulkIndexing {
	public static void main(String[] args) {
		String url = "jdbc:postgresql://130.83.164.196/";
		String dbName = "dividdj";
		String driver = "org.postgresql.Driver";
		String userName = "seid";
		String password = "seid";
		JsonWriter writer;
		Node node = nodeBuilder().node();
		Client client = node.client();
		BulkRequestBuilder bulkRequest = client.prepareBulk();

		documentIndexer(url, dbName, driver, userName, password);
	}

	private static void documentIndexer(String url, String dbName, String driver, String userName, String password) {
		JsonWriter writer;
		try {
			Class.forName(driver).newInstance();
			Connection conn = DriverManager.getConnection(url + dbName, userName, password);
			Statement st = conn.createStatement();
			ResultSet docSt = st.executeQuery("select * from document limit 3;");
			writer = new JsonWriter(new FileWriter("document.json"));
			writer.beginArray();
			while (docSt.next()) {
				String content = docSt.getString("content");
				Date created = docSt.getDate("created");
				Integer id = docSt.getInt("id");

				writer.beginObject();
				writer.name("id").value(id);
				writer.name("content").value(content);
				writer.name("created").value(created.toString());
				writer.endObject();
			}
			writer.endArray();
			writer.close();
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
