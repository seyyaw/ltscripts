package de.tudarmstadt.lt.nod;

import java.io.FileWriter;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.elasticsearch.node.NodeBuilder.*;

import com.google.gson.stream.JsonWriter;

public class PSQL2ESBulkIndexing {
	private static Connection conn;
	private static Statement st;
	static Logger logger = Logger.getLogger(PSQL2ESBulkIndexing.class.getName());

	public static void main(String[] args) throws Exception {

		initDB();
		Node node = nodeBuilder().node();
		Client client = node.client();
		// document2Json();
		documenIndexer(client, args[0]);
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

	private static void documenIndexer(Client client, String indexName) throws Exception {

		boolean exists = client.admin().indices().prepareExists(indexName).execute().actionGet().isExists();
		if (!exists) {
			createIndex(indexName, client);
		}
		ResultSet docSt = st.executeQuery("select * from document;");
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		int bblen = 0;
		while (docSt.next()) {
			String content = docSt.getString("content");
			Date created = docSt.getDate("created");
			Integer id = docSt.getInt("id");
			bulkRequest.add(client.prepareIndex(indexName, "document", id.toString()).setSource(
					jsonBuilder().startObject().field("content", content).field("created", created).endObject()));
			bblen++;
			if (bblen % 1000 == 0) {
				logger.info("##### " + bblen + " documents are indexed.");
				BulkResponse bulkResponse = bulkRequest.execute().actionGet();
				if (bulkResponse.hasFailures()) {
					logger.error("##### Bulk Request failure with error: " + bulkResponse.buildFailureMessage());
				}
				bulkRequest = client.prepareBulk();
			}
		}
		if (bulkRequest.numberOfActions() > 0) {
			logger.info("##### " + bblen + " data indexed.");
			BulkResponse bulkRes = bulkRequest.execute().actionGet();
			if (bulkRes.hasFailures()) {
				logger.error("##### Bulk Request failure with error: " + bulkRes.buildFailureMessage());
			}
		}

	}

	private static void initDB()
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		String url = "jdbc:postgresql://130.83.164.196/";
		String dbName = "dividdj";
		String driver = "org.postgresql.Driver";
		String userName = "seid";
		String password = "seid";
		Class.forName(driver).newInstance();
		conn = DriverManager.getConnection(url + dbName, userName, password);
		st = conn.createStatement();
	}

	public static void createIndex(String indexName, Client client) throws Exception {
		CreateIndexRequest request = new CreateIndexRequest(indexName);
		IndicesAdminClient iac = client.admin().indices();
		CreateIndexResponse response = iac.create(request).actionGet();
		if (!response.isAcknowledged()) {
			throw new Exception("Failed to delete index " + indexName);
		}
	}
}
