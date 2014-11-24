package es.official;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Before;
import org.junit.Test;

import es.ESTestBase;

public class QueryDslFilterOfficial extends ESTestBase {

  private SearchRequestBuilder srb;

  @Before
  public void setupForQuery() {
    srb = client.prepareSearch(indexShakeSpeare);
  }

  @Test
  public void testAndFilter() {
    // basic steps:
    // 1. create FilterBuilder by FilterBuilders (optionally caching)
    FilterBuilder andFilter =
        FilterBuilders.andFilter(FilterBuilders.rangeFilter("speech_number").from(2).to(5),
            FilterBuilders.prefixFilter("text_entry", "against")).cache(true);

    // 2. create filteredQuery type query and then add filter
    QueryBuilder filteredQuery =
        QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), andFilter);

    // 3. execute the search as usual
    SearchResponse response = srb.setQuery(filteredQuery).execute().actionGet();

    // read response
    long totalMatched = response.getHits().getTotalHits();
    System.out.println("Total matched: " + totalMatched);
  }

  @Test
  public void testBoolFilter() {
    FilterBuilder boolFilter =
        FilterBuilders.boolFilter().must(FilterBuilders.termFilter("speaker", "WESTMORELAND"))
            .mustNot(FilterBuilders.termFilter("speech_number", 2));

    QueryBuilder filteredQuery =
        QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), boolFilter);

    // 3. execute the search as usual
    SearchResponse response = srb.setQuery(filteredQuery).execute().actionGet();

    // read response
    long totalMatched = response.getHits().getTotalHits();
    System.out.println("Total matched: " + totalMatched);
  }

  // combine some basic query as well
  @Test
  public void testBoolFilterWithMatchQuery() {
    FilterBuilder boolFilter =
        FilterBuilders.boolFilter().must(FilterBuilders.termFilter("speaker", "WESTMORELAND"))
            .mustNot(FilterBuilders.termFilter("speech_number", 2));

    QueryBuilder filteredQuery =
        QueryBuilders.filteredQuery(QueryBuilders.matchQuery("text_entry", "day"), boolFilter);

    // 3. execute the search as usual
    SearchResponse response = srb.setQuery(filteredQuery).execute().actionGet();

    // read response
    long totalMatched = response.getHits().getTotalHits();
    System.out.println("Total matched: " + totalMatched);
  }

  @Test
  public void testExistsFilter() {
    // http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-exists-filter.html
    // FilterBuilders.existsFilter("user");
    // TODO
  }

  @Test
  public void testIdsFilter() {
    // type parameter in idsFilter is optional, search all types in the target index
    FilterBuilder idsFilter = FilterBuilders.idsFilter().addIds("1", "2");

    QueryBuilder filteredQuery =
        QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), idsFilter);

    SearchResponse response = srb.setQuery(filteredQuery).execute().actionGet();

    // read response
    long totalMatched = response.getHits().getTotalHits();
    System.out.println("Total matched: " + totalMatched);
  }

  @Test
  public void testLimitFilter() {
    // actual overall limit value is size * number_of_shards
    FilterBuilder limitFilter = FilterBuilders.limitFilter(30);

    QueryBuilder filteredQuery =
        QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), limitFilter);

    SearchResponse response = srb.setQuery(filteredQuery).execute().actionGet();

    // read response
    long totalMatched = response.getHits().getTotalHits();
    System.out.println("Total matched: " + totalMatched);
  }

  @Test
  public void testTypeFilter() {
    FilterBuilder typeFilter = FilterBuilders.typeFilter("line");

    QueryBuilder filteredQuery =
        QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), typeFilter);

    // set search type to COUNT
    SearchResponse response =
        srb.setSearchType(SearchType.COUNT).setQuery(filteredQuery).execute().actionGet();

    // read response directly
    System.out.println("Count search response: " + response);
  }

  // TODO: some geo-related filters

  @Test
  public void testHasChildParentFilter() {
    // http://www.elasticsearch.org/guide/en/elasticsearch/client/java-api/current/query-dsl-filters.html#has-child-parent-filter
    // TODO
  }

  @Test
  public void testMatchAllFilter() {
    FilterBuilder matchAllFilter = FilterBuilders.matchAllFilter();
    QueryBuilder constantScoreQuery = QueryBuilders.constantScoreQuery(matchAllFilter);

    SearchResponse response = srb.setQuery(constantScoreQuery).execute().actionGet();

    // read response directly
    System.out.println("Search response: " + response);
  }

  @Test
  public void testMissingFilter() {
    // http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-missing-filter.html
    // TODO
  }

  @Test
  public void testNotFilter() {
    FilterBuilder notFilter =
        FilterBuilders.notFilter(FilterBuilders.rangeFilter("speech_number").from(2).to(5)).cache(
            true);

    QueryBuilder filteredQuery =
        QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), notFilter);

    SearchResponse response = srb.setQuery(filteredQuery).execute().actionGet();

    // read response
    long totalMatched = response.getHits().getTotalHits();
    System.out.println("Total matched: " + totalMatched);
  }

  @Test
  public void testOrFilter() {
    FilterBuilder orFilter =
        FilterBuilders.orFilter(FilterBuilders.rangeFilter("speech_number").from(2).to(5),
            FilterBuilders.prefixFilter("text_entry", "against")).cache(true);

    QueryBuilder filteredQuery =
        QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), orFilter);

    SearchResponse response = srb.setQuery(filteredQuery).execute().actionGet();

    // read response
    long totalMatched = response.getHits().getTotalHits();
    System.out.println("Total matched: " + totalMatched);
  }

  @Test
  public void testPrefixFilter() {
    FilterBuilder prefixFilter = FilterBuilders.prefixFilter("text_entry", "against");

    QueryBuilder filteredQuery =
        QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), prefixFilter);

    SearchResponse response = srb.setQuery(filteredQuery).execute().actionGet();

    // read response
    long totalMatched = response.getHits().getTotalHits();
    System.out.println("Total matched: " + totalMatched);
  }

  @Test
  public void testRangeFilter() {
    // FilterBuilders.rangeFilter("age").from("10").to("20").includeLower(true).includeUpper(false);

    // A simplified form using gte, gt, lt or lte
    // FilterBuilders.rangeFilter("age").gte("10").lt("20");

    // TODO
  }

  @Test
  public void testScriptFilter() {
    // http://www.elasticsearch.org/guide/en/elasticsearch/client/java-api/current/query-dsl-filters.html#script-filter
    // http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-script-filter.html
    // TODO
  }

  @Test
  public void testTermFilter() {
    FilterBuilder termFilter = FilterBuilders.termFilter("text_entry", "havoc");

    QueryBuilder filteredQuery =
        QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), termFilter);

    SearchResponse response = srb.setQuery(filteredQuery).execute().actionGet();

    // read response
    long totalMatched = response.getHits().getTotalHits();
    System.out.println("Total matched: " + totalMatched);
  }

  @Test
  public void testTermsFilter() {
    // execution:
    // http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-terms-filter.html#_execution_mode
    FilterBuilder termFilter =
        FilterBuilders.termsFilter("text_entry", "havoc", "gallant").execution("plain");

    QueryBuilder filteredQuery =
        QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), termFilter);

    SearchResponse response = srb.setQuery(filteredQuery).execute().actionGet();

    // read response
    long totalMatched = response.getHits().getTotalHits();
    System.out.println("Total matched: " + totalMatched);
  }

  @Test
  public void testNestedFilter() {
    // http://www.elasticsearch.org/guide/en/elasticsearch/client/java-api/current/query-dsl-filters.html#nested-filter
    // TODO
  }
}
