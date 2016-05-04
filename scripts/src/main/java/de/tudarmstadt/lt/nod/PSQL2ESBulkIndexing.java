package de.tudarmstadt.lt.nod;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
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

public class PSQL2ESBulkIndexing
{
    private static Connection conn;
    private static Statement st;
    static Logger logger = Logger.getLogger(PSQL2ESBulkIndexing.class.getName());

    public static void main(String[] args)
        throws Exception
    {
        String usage = "Run as: java -jar dbname indexname elasticsearch.yml";
        if (args.length == 0) {
            logger.error("please provide dbname name");
            logger.error(usage);
            System.exit(1);
        }
        if (args.length == 1) {
            logger.error("please provide index name ");
            logger.error(usage);
            System.exit(1);
        }
        if (args.length == 2) {
            logger.error("please provide elasticsearch.yml file ");
            logger.error(usage);
            System.exit(1);
        }
        initDB(args[0], "", "seid", "seid");
        st.setFetchSize(50);
        Path path = new File(args[2]).toPath();
        Settings settings = settingsBuilder().loadFromPath(path).build();

        Node node = nodeBuilder().settings(settings).node();
        Client client = node.client();
        // document2Json();
        documenIndexer(client, args[1], "document");
    }

    private static void document2Json()
    {
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
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void documenIndexer(Client client, String indexName, String documentType)
        throws Exception
    {
        try {
            boolean exists = client.admin().indices().prepareExists(indexName).execute().actionGet()
                    .isExists();
            if (!exists) {
                System.out.println("Index " + indexName + " will be created.");
                // createIndex(indexName, client);
                createcableIndex(client, indexName, documentType);
                // createEnronIndex(client, indexName, documentType);

                System.out.println("Index " + indexName + " is created.");
            }
        }
        catch (Exception e) {
            // starnange error
            logger.error(e.getMessage());
        }

        ResultSet docSt = st.executeQuery("select * from document;");
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        int bblen = 0;
        List<NamedEntity> namedEntity = new ArrayList<>();
        while (docSt.next()) {
            String content = docSt.getString("content");
            Date created = docSt.getDate("created");
            Integer docId = docSt.getInt("id");

            ResultSet docEntSt = conn.createStatement()
                    .executeQuery("select * from documententity where  docid = " + docId + ";");
            while (docEntSt.next()) {
                long entId = docEntSt.getLong("entityid");
                ResultSet entSt = conn.createStatement()
                        .executeQuery("select * from entity where  id = " + entId + ";");
                if (entSt.next()) {
                    NamedEntity ne = new NamedEntity(entSt.getLong("id"), entSt.getString("name"),
                            entSt.getString("type"), docEntSt.getInt("frequency"));
                    namedEntity.add(ne);
                }

            }

           /* ResultSet docRelSt = conn.createStatement().executeQuery(
                    "select * from documentrelationship where  docid = " + docId + ";");
            Map<Long, String> rels = new HashMap<>();
            Map<Long, Integer> relIds = new HashMap<>();
            while (docRelSt.next()) {
                long relId = docRelSt.getLong("relid");
                ResultSet relSt = conn.createStatement()
                        .executeQuery("select * from relationship where  id = " + relId + ";");
                if (relSt.next()) {
                    rels.put(relId, relSt.getLong("entity1") + ":" + relSt.getLong("entity2"));
                    relIds.put(relId, docRelSt.getInt("frequency"));
                }

            }*/

            ResultSet metadataSt = conn.createStatement()
                    .executeQuery("select * from metadata where docid =" + docId + ";");

            XContentBuilder xb = XContentFactory.jsonBuilder().startObject();
            xb.field("content", content).field("created", created);
            Map<String, List<String>> metas = new HashMap<>();
            while (metadataSt.next()) {
                String key = metadataSt.getString("key").replace(".", "_");
                String value = metadataSt.getString("value");
                // Object type = metadataSt.getObject("type");
                // xb.field(key, value)/* .field("value",
                // value).field("type",type) */;
                metas.putIfAbsent(key, new ArrayList<>());
                metas.get(key).add(value);
            }
            for (String key : metas.keySet()) {
                if (metas.get(key).size() > 1) { // array field
                    xb.field(key, metas.get(key));
                }
                else {
                    xb.field(key, metas.get(key).get(0));
                }
            }
            if (namedEntity.size() > 0) {
                xb.startArray("entities");
                for (NamedEntity ne : namedEntity) {
                    xb.startObject();
                    xb.field("id", ne.id);
                    xb.field("name", ne.name);
                    xb.field("Entitytype", ne.type);
                    xb.field("frequency", ne.frequency);
                    xb.endObject();
                }
                xb.endArray();
            }

       /*     if (rels.size() > 0) {
                xb.startArray("relations");
                for (Long relId : rels.keySet()) {
                    xb.startObject();
                    xb.field("id", relId);
                    xb.field("relation", rels.get(relId));
                    xb.field("frequency", relIds.get(relId));
                    xb.endObject();
                }
                xb.endArray();
            }*/
            xb.endObject();
            metadataSt.close();
            bulkRequest.add(
                    client.prepareIndex(indexName, documentType, docId.toString()).setSource(xb));
            bblen++;
            if (bblen % 1000 == 0) {
                logger.info("##### " + bblen + " documents are indexed.");
                BulkResponse bulkResponse = bulkRequest.execute().actionGet();
                if (bulkResponse.hasFailures()) {
                    logger.error("##### Bulk Request failure with error: "
                            + bulkResponse.buildFailureMessage());
                }
                bulkRequest = client.prepareBulk();
            }
        }
        docSt.close();
        if (bulkRequest.numberOfActions() > 0) {
            logger.info("##### " + bblen + " data indexed.");
            BulkResponse bulkRes = bulkRequest.execute().actionGet();
            if (bulkRes.hasFailures()) {
                logger.error(
                        "##### Bulk Request failure with error: " + bulkRes.buildFailureMessage());
            }
        }

    }

    public static void createIndex(String indexName, Client client)
        throws Exception
    {
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        IndicesAdminClient iac = client.admin().indices();

        CreateIndexResponse response = iac.create(request).actionGet();
        if (!response.isAcknowledged()) {
            throw new Exception("Failed to delete index " + indexName);
        }
    }

    // Based on this so:
    // http://stackoverflow.com/questions/22071198/adding-mapping-to-a-type-from-java-how-do-i-do-it

    public static void createcableIndex(Client client, String indexName, String documentType)
        throws IOException
    {

        IndicesExistsResponse res = client.admin().indices().prepareExists(indexName).execute()
                .actionGet();
        if (res.isExists()) {
            DeleteIndexRequestBuilder delIdx = client.admin().indices().prepareDelete(indexName);
            delIdx.execute().actionGet();
        }

        CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices()
                .prepareCreate(indexName);

        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder().startObject()
                .startObject(documentType).startObject("properties").startObject("content")
                .field("type", "string").field("analyzer", "english")
                .field("term_vector", "with_positions_offsets_payloads").field("store", "yes")
                .endObject().startObject("Subject").field("type", "string")
                .field("analyzer", "english").endObject().startObject("Header")
                .field("type", "string").field("analyzer", "english").endObject()
                .startObject("Origin").field("type", "string").field("index", "not_analyzed")
                .endObject().startObject("Classification").field("type", "string")
                .field("index", "not_analyzed").endObject().startObject("ReferenceId")
                .field("type", "string").field("index", "not_analyzed").endObject()
                .startObject("References").field("type", "string").field("store", "yes")
                .field("index", "not_analyzed").endObject().startObject("SignedBy")
                .field("type", "string").field("store", "yes").field("index", "not_analyzed")
                .endObject().startObject("Tags").field("type", "string").field("store", "yes")
                .field("index", "not_analyzed").endObject()

                .startArray("entities").field("store", "yes").startObject("id")
                .field("type", "integer").field("index", "not_analyzed").endObject().startObject("name")
                .field("type", "string").field("index", "not_analyzed").endObject()
                .startObject("Entitytype").field("type", "string").field("index", "not_analyzed")
                .endObject().startObject("frequency").field("type", "integer")
                .field("index", "not_analyzed").endObject().endArray()

                /*.startArray("relations").field("store", "yes").startObject("id")
                .field("type", "integer").field("index", "not_analyzed").endObject()
                .startObject("relation").field("type", "string").field("index", "not_analyzed")
                .endObject().startObject("frequency").field("type", "integer")
                .field("index", "not_analyzed").endObject().endArray()*/.endObject().endObject();
        createIndexRequestBuilder.addMapping(documentType, mappingBuilder);

        createIndexRequestBuilder.execute().actionGet();
    }

    public static void createEnronIndex(Client client, String indexName, String documentType)
        throws Exception
    {

        IndicesExistsResponse res = client.admin().indices().prepareExists(indexName).execute()
                .actionGet();
        if (res.isExists()) {
            DeleteIndexRequestBuilder delIdx = client.admin().indices().prepareDelete(indexName);
            delIdx.execute().actionGet();
        }

        CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices()
                .prepareCreate(indexName);

        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder().startObject()
                .startObject(documentType).startObject("properties").startObject("content")
                .field("type", "string").field("analyzer", "english").endObject()
                .startObject("Subject").field("type", "string")
                .field("term_vector", "with_positions_offsets_payloads").field("store", "yes")
                .field("analyzer", "english").endObject()

                .startObject("Timezone").field("type", "string").field("index", "not_analyzed")
                .endObject().startObject("Recipients_name").field("type", "string")
                .field("store", "yes").field("index", "not_analyzed").endObject()
                .startObject("Recipients_email").field("type", "string").field("store", "yes")
                .field("index", "not_analyzed").endObject().startObject("Recipients_order")
                .field("type", "short").field("store", "yes").field("index", "not_analyzed")
                .endObject().startObject("Recipients_type").field("type", "string")
                .field("store", "yes").field("index", "not_analyzed").endObject()
                .startObject("Recipients_id").field("type", "long").field("store", "yes")
                .field("index", "not_analyzed").endObject()

                .startObject("sender_id").field("type", "long").field("index", "not_analyzed")
                .endObject().startObject("sender_email").field("type", "string")
                .field("index", "not_analyzed").endObject().startObject("sender_name")
                .field("type", "string").field("index", "not_analyzed")

                .startArray("entities").field("store", "yes").startObject("id")
                .field("type", "integer").field("index", "not_analyzed").endObject()
                .startObject("name").field("type", "string").field("index", "not_analyzed")
                .endObject().startObject("Entitytype").field("type", "string")
                .field("index", "not_analyzed").endObject().startObject("frequency")
                .field("type", "integer").field("index", "not_analyzed").endObject().endArray()

                /*.startArray("relations").field("store", "yes").startObject("id")
                .field("type", "integer").field("index", "not_analyzed").endObject()
                .startObject("relation").field("type", "string").field("index", "not_analyzed")
                .endObject().startObject("frequency").field("type", "integer")
                .field("index", "not_analyzed").endObject().endArray()*/.endObject().endObject();
        createIndexRequestBuilder.addMapping(documentType, mappingBuilder);

        try {
            CreateIndexResponse response = createIndexRequestBuilder.execute().actionGet();
            if (!response.isAcknowledged()) {
                throw new Exception("Failed to delete index " + indexName);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void initDB(String aDbName, String ip, String user, String pswd)
        throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
    {
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

    static class NamedEntity
    {
        long id;
        String name;
        String type;
        int frequency;

        public NamedEntity(long aId, String aName, String aType, int aFreq)
        {
            this.id = aId;
            this.name = aName;
            this.type = aType;
            this.frequency = aFreq;

        }
    }
}