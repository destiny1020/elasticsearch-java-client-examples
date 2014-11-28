package es.official.api;

import org.elasticsearch.action.get.GetResponse;
import org.junit.Test;

import es.ESTestBase;

/**
 * http://www.elasticsearch.org/guide/en/elasticsearch/client/java-api/current/get.html
 */
public class GetApiOfficial extends ESTestBase {

  @Test
  public void testGetDocument() {
    GetResponse getResponse = client.prepareGet(indexName, typeName, "1")
    // used to execute the get operation on the calling thread
    // .setOperationThreaded(false)
        .execute().actionGet();

    System.out.println("Response: " + getResponse.getSourceAsString());
    // getResponse
    // .forEach((GetField field) -> {
    // System.out.println(String.format("Field: %s, Value: %s", field.getName(),
    // field.getValues()));
    // });
  }
}
