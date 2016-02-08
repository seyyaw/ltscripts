package de.tudarmstadt.lt.nod;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.elasticsearch.node.NodeBuilder.*;

import com.google.gson.stream.JsonWriter;

public class PSQL2ESBulkIndexing {
	private static Connection conn;
	private static Statement st;

	public static void main(String[] args)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, IOException {
		String url = "jdbc:postgresql://130.83.164.196/";
		String dbName = "dividdj";
		String driver = "org.postgresql.Driver";
		String userName = "seid";
		String password = "seid";
		init(url, dbName, driver, userName, password);
		JsonWriter writer;
		Node node = nodeBuilder()
				.settings(Settings.builder().put("path.home", "/media/seid/DATA/apps/elasticsearch-2.2.0/")).local(true)
				.clusterName("localhost:9200").node();
		Client client = node.client();
		document2Json();
		documenIndexer(client);
	}

	private static void document2Json() {
		JsonWriter writer;
		try {
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

	private static void documenIndexer(Client client) throws IOException, SQLException {

		ResultSet docSt = st.executeQuery("select * from document limit 3;");
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		while (docSt.next()) {
			String content = docSt.getString("content");
			Date created = docSt.getDate("created");
			Integer id = docSt.getInt("id");
			bulkRequest.add(client.prepareIndex("news_leaks", "document", id.toString()).setSource(
					jsonBuilder().startObject().field("content", content).field("created", created).endObject()));
		}

		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		if (bulkResponse.hasFailures()) {
			System.out.println(bulkRequest.get());
		}

	}

	private static void init(String url, String dbName, String driver, String userName, String password)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		Class.forName(driver).newInstance();
		conn = DriverManager.getConnection(url + dbName, userName, password);
		st = conn.createStatement();
	}
}
