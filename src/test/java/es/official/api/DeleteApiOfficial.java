package es.official.api;

import org.elasticsearch.action.delete.DeleteResponse;
import org.junit.Test;

import es.ESTestBase;

/**
 * http://www.elasticsearch.org/guide/en/elasticsearch/client/java-api/current/delete.html
 */
public class DeleteApiOfficial extends ESTestBase {

  @Test
  public void testDeleteDocument() {
    // Test 1: delete document
    DeleteResponse response = client.prepareDelete(indexName, typeName, "1").execute().actionGet();

    System.out.println("Response - Is Found:" + response.isFound());
  }

}
