package es.official;

import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;

import es.ESTestBase;

/**
 * http://www.elasticsearch.org/guide/en/elasticsearch/client/java-api/current/delete-by-query.html
 */
public class DeleteByQueryApiOfficial extends ESTestBase {

  @Test
  public void testDeleteByQuery() {
    DeleteByQueryResponse response =
        client.prepareDeleteByQuery(indexName).setQuery(QueryBuilders.termQuery("message", "too"))
            .execute().actionGet();

    System.out.println(response.toString());
  }

}
