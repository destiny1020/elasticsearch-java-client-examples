package es.official.api;

import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;

import es.ESTestBase;

/**
 * http://www.elasticsearch.org/guide/en/elasticsearch/client/java-api/current/search.html
 */
public class SearchApiOfficial extends ESTestBase {

  @Test
  public void testSearch() {
    SearchResponse searchResponse =
        client.prepareSearch(indexName).setTypes(typeName)
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            // set query
            .setQuery(QueryBuilders.termQuery("message", "try"))
            // set filter
            .setPostFilter(FilterBuilders.rangeFilter("age").from(15).to(25)).setFrom(0)
            .setSize(60).setExplain(true).execute().actionGet();

    // smallest search call
    // SearchResponse searchResponse = client.prepareSearch(indexName).execute().actionGet();

    // read response
    System.out.println(searchResponse.toString());
  }

  /**
   * scroll search on a bigger index
   */
  @Test
  public void testScrollSearch() {
    TermQueryBuilder termQuery = QueryBuilders.termQuery(fieldShakeSpeare, "henry");

    SearchResponse scrollResponse =
        client.prepareSearch(indexShakeSpeare).setSearchType(SearchType.SCAN)
            .setScroll(new TimeValue(60000)).setQuery(termQuery)
            // actual retrieval size: number_of_shards * size = 5 * 40 = 200, in this case
            .setSize(40).execute().actionGet();

    // scroll until no hits are returned
    int searchTime = 1;
    while (true) {
      System.out.println("Search Time: " + searchTime++);
      for (SearchHit hit : scrollResponse.getHits()) {
        // handle hit
        System.out.println(hit.sourceAsString());
      }

      scrollResponse =
          client.prepareSearchScroll(scrollResponse.getScrollId()).setScroll(new TimeValue(60000))
              .execute().actionGet();

      // first getHits returns SearchHits, second getHits returns SearchHit[]
      if (scrollResponse.getHits().getHits().length == 0) {
        break;
      }
    }
  }

  @Test
  public void testMultiSearch() {
    // query string type
    SearchRequestBuilder srb1 =
        client.prepareSearch(indexShakeSpeare).setQuery(QueryBuilders.queryString("elephant"))
        // size: 10 is the default
            .setSize(10);

    // match query type
    SearchRequestBuilder srb2 =
        client.prepareSearch(indexShakeSpeare)
            .setQuery(QueryBuilders.matchQuery(fieldShakeSpeare, "havoc")).setSize(10);

    // correlate the srbs
    MultiSearchResponse sr = client.prepareMultiSearch().add(srb1).add(srb2).execute().actionGet();

    // get all individual responses from MultiSearchResponse
    long totalHits = 0;
    for (MultiSearchResponse.Item item : sr.getResponses()) {
      SearchResponse response = item.getResponse();
      totalHits += response.getHits().getTotalHits();
    }

    System.out.println("Total matching for the elephant and havoc: " + totalHits);
  }

  @Test
  public void testFacets() {
    // TODO: Facet API
    // http://www.elasticsearch.org/guide/en/elasticsearch/client/java-api/current/java-facets.html

  }

}
