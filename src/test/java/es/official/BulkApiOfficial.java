package es.official;

import java.io.IOException;
import java.util.Date;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;

import es.ESTestBase;

public class BulkApiOfficial extends ESTestBase {

  @Test
  public void testBulkRequest() throws IOException {
    BulkRequestBuilder bulkRequest = client.prepareBulk();

    // prepare json builder
    XContentBuilder builder1 =
        XContentFactory.jsonBuilder().startObject().field("user", "destiny1020")
            .field("postDate", new Date()).field("message", "Try ES !").field("age", 20)
            .endObject();

    XContentBuilder builder2 =
        XContentFactory.jsonBuilder().startObject().field("user", "maruko0101")
            .field("postDate", new Date()).field("message", "Try ES Too !").field("age", 10)
            .endObject();

    // prepare index request
    IndexRequestBuilder irb1 = client.prepareIndex(indexName, typeName, "1").setSource(builder1);
    IndexRequestBuilder irb2 = client.prepareIndex(indexName, typeName, "2").setSource(builder2);

    bulkRequest.add(irb1);
    bulkRequest.add(irb2);

    BulkResponse bulkResponse = bulkRequest.execute().actionGet();
    if (bulkResponse.hasFailures()) {
      // process failures by iterating through each bulk response item
      System.err.println("Has failures");
    } else {
      System.out.println("Bulk OK !");
    }
  }
}
