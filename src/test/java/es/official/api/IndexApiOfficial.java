package es.official.api;

import java.io.IOException;
import java.util.Date;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;

import es.ESTestBase;

/**
 * http://www.elasticsearch.org/guide/en/elasticsearch/client/java-api/current/index_.html
 */
public class IndexApiOfficial extends ESTestBase {

  @Test
  public void testIndex() throws IOException {
    // generate json document
    // 1. Manually (aka do it yourself) using native byte[] or as a String
    // 2. Using a Map that will be automatically converted to its JSON equivalent
    // 3. Using a third party library to serialize your beans such as Jackson
    // 4. Using built-in helpers XContentFactory.jsonBuilder()

    // Test 1: generate json doc by built-in helper
    XContentBuilder builder =
        XContentFactory.jsonBuilder().startObject().field("user", "destiny1020")
            .field("postDate", new Date()).field("message", "Try ES").endObject();

    System.out.println(builder.string());

    // Test 2: index document
    String indexName = "itest";
    String typeName = "ttest";

    IndexResponse indexResponse =
        client.prepareIndex(indexName, typeName, "1").setSource(builder).execute().actionGet();

    if (indexResponse != null && indexResponse.isCreated()) {
      System.out.println("Index has been created !");

      // read report from response
      System.out.println("Index name: " + indexResponse.getIndex());
      System.out.println("Type name: " + indexResponse.getType());
      System.out.println("ID(optional): " + indexResponse.getId());
      System.out.println("Version: " + indexResponse.getVersion());
    } else {
      System.err.println("Index creation failed.");
    }

  }

}
