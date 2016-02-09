package de.tudarmstadt.lt.nod;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
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
import org.elasticsearch.common.collect.HppcMaps;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;

import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.elasticsearch.node.NodeBuilder.*;

import com.google.gson.stream.JsonWriter;
import org.json.JSONArray;
import org.json.JSONObject;

public class PSQL2ESBulkIndexing {
    private static Connection conn;
    private static Statement st;
    static Logger logger = Logger.getLogger(PSQL2ESBulkIndexing.class.getName());

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            logger.error("please provide index name");
            System.exit(1);
        }
        if (args.length == 1) {
            logger.error("please provide elasticsearch.yml file ");
            System.exit(1);
        }
        initDB();
        Path path = new File(args[1]).toPath();
        Settings settings = settingsBuilder().loadFromPath(path).build();

        Node node = nodeBuilder().settings(settings).node();
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
        try {
            boolean exists = client.admin().indices().prepareExists(indexName).execute().actionGet().isExists();
            if (!exists) {
                System.out.println("CREATING WRONG");
                createIndex(indexName, client);
            }
        } catch (Exception e) {
            // starnange error
            logger.error(e.getMessage());
        }
        ResultSet docSt = st.executeQuery("select * from document;");
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        int bblen = 0;
        while (docSt.next()) {
            String content = docSt.getString("content");
            Date created = docSt.getDate("created");
            Integer id = docSt.getInt("id");
            
            ResultSet metadataSt = conn.createStatement().executeQuery("select * from metadata where docid =" + id + ";");

            JSONArray metadataArrys = new JSONArray();
            while (metadataSt.next()) {
                String key = metadataSt.getString("key");
                Object value = metadataSt.getObject("value");
                Object type = metadataSt.getObject("type");

                JSONObject metadata = new JSONObject();
                metadata.put("key", key);
                metadata.put("value", value);
                metadata.put("type", type);

                metadataArrys.put(metadata);

            }
            metadataSt.close();
            bulkRequest.add(client.prepareIndex(indexName, "document", id.toString()).setSource(
                    jsonBuilder().startObject().field("content", content).field("created", created).field("metadata",metadataArrys).endObject()));
            bblen++;
            if (bblen % 100 == 0) {
                logger.info("##### " + bblen + " documents are indexed.");
                BulkResponse bulkResponse = bulkRequest.execute().actionGet();
                if (bulkResponse.hasFailures()) {
                    logger.error("##### Bulk Request failure with error: " + bulkResponse.buildFailureMessage());
                }
                bulkRequest = client.prepareBulk();
            }
        }
        docSt.close();
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
