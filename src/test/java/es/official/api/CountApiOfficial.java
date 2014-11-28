package es.official.api;

import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;

import es.ESTestBase;

/**
 * http://www.elasticsearch.org/guide/en/elasticsearch/client/java-api/current/count.html
 */
public class CountApiOfficial extends ESTestBase {

  private String type = "line";

  @Test
  public void testCount() {

    CountResponse response =
        client.prepareCount(indexShakeSpeare).setQuery(QueryBuilders.termQuery("_type", type))
            .execute().actionGet();

    System.out.println(String.format("Count for type %s is %d", type, response.getCount()));

  }

}
