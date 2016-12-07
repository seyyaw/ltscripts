package de.tudarmstadt.lt.nod;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;

public class MyTestClass {

    public static void main(final String[] args) throws IOException, InterruptedException {

    	Path path = new File("elasticsearch.yml").toPath();
		Settings settings = settingsBuilder().loadFromPath(path).build();

		Node node = nodeBuilder().settings(settings).node();
		Client client = node.client();
    	client.admin().indices().create(new CreateIndexRequest("indexname")).actionGet();
    	XContentBuilder mapping = jsonBuilder()
                .startObject()
                     .startObject("general")
                          .startObject("properties")
                              .startObject("message")
                                  .field("type", "string")
                                  .field("index", "not_analyzed")
                               .endObject()
                               .startObject("source")
                                  .field("type","string")
                               .endObject()
                          .endObject()
                      .endObject()
                   .endObject();

PutMappingResponse putMappingResponse = client.admin().indices()
  .preparePutMapping("test")
  .setType("general")
  .setSource(mapping)
  .execute().actionGet();
    }

}
