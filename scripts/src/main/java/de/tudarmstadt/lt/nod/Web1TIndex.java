package de.tudarmstadt.lt.nod;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;

import static org.elasticsearch.node.NodeBuilder.*;

import com.google.gson.stream.JsonWriter;

public class Web1TIndex {
	static Logger logger = Logger.getLogger(Web1TIndex.class.getName());

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			logger.error("please provide index name");
			System.exit(1);
		}
		if (args.length == 1) {
			logger.error("please provide elasticsearch.yml file ");
			System.exit(1);
		}
		Path path = new File(args[1]).toPath();
		Settings settings = settingsBuilder().loadFromPath(path).build();

		Node node = nodeBuilder().settings(settings).node();
		Client client = node.client();
		documenIndexer(client, args[0], "document", new File(args[2]));
	}

	private static void documenIndexer(Client client, String indexName, String documentType, File web1TDir)
			throws Exception {
		try {
			boolean exists = client.admin().indices().prepareExists(indexName).execute().actionGet().isExists();
			if (!exists) {
				System.out.println("Index " + indexName + " will be created.");
				createEnronIndex(client, indexName, documentType);

				System.out.println("Index " + indexName + " is created.");
			}
		} catch (Exception e) {
			// starnange error
			logger.error(e.getMessage());
		}

		Files.walk(Paths.get(web1TDir.getAbsolutePath())).forEach(filePath -> {
			if (Files.isRegularFile(filePath)) {
				try {
					BulkRequestBuilder bulkRequest = client.prepareBulk();
					int bblen = 0;
					try (Stream<String> lines = Files.lines (filePath, StandardCharsets.UTF_8))
					{
					    for (String line : (Iterable<String>) lines::iterator)
					    {
						String ngram = line.split("\t")[0];
						Long count = Long.valueOf(line.split("\t")[1]);
						XContentBuilder xb = XContentFactory.jsonBuilder().startObject();
						xb.field("ngram", ngram).field("count", count);
						xb.endObject();
						bulkRequest.add(client.prepareIndex(indexName, documentType).setSource(xb));
						bblen++;
						if (bblen % 100000 == 0) {
							logger.info("##### " + bblen + " documents are indexed.");
							BulkResponse bulkResponse = bulkRequest.execute().actionGet();
							if (bulkResponse.hasFailures()) {
								logger.error(
										"##### Bulk Request failure with error: " + bulkResponse.buildFailureMessage());
							}
							bulkRequest = client.prepareBulk();
						}
					}
					}
					if (bulkRequest.numberOfActions() > 0) {
						logger.info("##### " + bblen + " data indexed.");
						BulkResponse bulkRes = bulkRequest.execute().actionGet();
						if (bulkRes.hasFailures()) {
							logger.error("##### Bulk Request failure with error: " + bulkRes.buildFailureMessage());
						}
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		});

	}

	public static void createIndex(String indexName, Client client) throws Exception {
		CreateIndexRequest request = new CreateIndexRequest(indexName);
		IndicesAdminClient iac = client.admin().indices();

		CreateIndexResponse response = iac.create(request).actionGet();
		if (!response.isAcknowledged()) {
			throw new Exception("Failed to delete index " + indexName);
		}
	}

	public static void createEnronIndex(Client client, String indexName, String documentType) throws Exception {

		IndicesExistsResponse res = client.admin().indices().prepareExists(indexName).execute().actionGet();
		if (res.isExists()) {
			DeleteIndexRequestBuilder delIdx = client.admin().indices().prepareDelete(indexName);
			delIdx.execute().actionGet();
		}

		CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(indexName);

		XContentBuilder mappingBuilder = XContentFactory.jsonBuilder().startObject().startObject(documentType)
				.startObject("properties").startObject("ngram").field("type", "string").field("analyzer", "english")
				.endObject().startObject("count").field("type", "long").field("index", "not_analyzed").endObject()
				.endObject().endObject();
		createIndexRequestBuilder.addMapping(documentType, mappingBuilder);

		try {
			CreateIndexResponse response = createIndexRequestBuilder.execute().actionGet();
			if (!response.isAcknowledged()) {
				throw new Exception("Failed to delete index " + indexName);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
