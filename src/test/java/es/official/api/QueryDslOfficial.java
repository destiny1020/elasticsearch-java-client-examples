package es.official.api;

import java.util.Arrays;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Before;
import org.junit.Test;

import es.ESTestBase;

/**
 * http://www.elasticsearch.org/guide/en/elasticsearch/client/java-api/current/query-dsl-queries.
 * html
 */
public class QueryDslOfficial extends ESTestBase {

  private SearchRequestBuilder srb;

  @Before
  public void setupForQuery() {
    srb = client.prepareSearch(indexShakeSpeare);
  }

  @Test
  public void testMatchQuery() {
    QueryBuilder qb = QueryBuilders.matchQuery(fieldShakeSpeare, "havoc");

    SearchResponse response = srb.setQuery(qb).execute().actionGet();

    // read response
    int totalMatched = response.getHits().getHits().length;
    System.out.println("Total matched: " + totalMatched);
  }

  @Test
  public void testMultiMatchQuery() {
    // Text you are looking for and fields you query on
    QueryBuilder qb = QueryBuilders.multiMatchQuery("henry", "text_entry", "play_name");

    SearchResponse response = srb.setQuery(qb).execute().actionGet();

    // read response
    long totalMatched = response.getHits().getTotalHits();
    System.out.println("Total matched: " + totalMatched);
  }

  @Test
  public void testBooleanQuery() {
    QueryBuilder qb =
        QueryBuilders.boolQuery().must(QueryBuilders.termQuery(fieldShakeSpeare, "love"))
            .must(QueryBuilders.termQuery(fieldShakeSpeare, "henry"))
            .mustNot(QueryBuilders.termQuery(fieldShakeSpeare, "and"))
            .should(QueryBuilders.termQuery(fieldShakeSpeare, "sole"));

    SearchResponse response = srb.setQuery(qb).execute().actionGet();

    // read response
    long totalMatched = response.getHits().getTotalHits();
    System.out.println("Total matched: " + totalMatched);
  }

  @Test
  public void testBoostingQuery() {
    QueryBuilder qb =
        QueryBuilders.boostingQuery().positive(QueryBuilders.termQuery(fieldShakeSpeare, "henry"))
            .negative(QueryBuilders.termQuery(fieldShakeSpeare, "exit")).negativeBoost(0.3f);

    SearchResponse response = srb.setQuery(qb).execute().actionGet();

    // read response
    printTotalAndFirstPage(response);
  }

  @Test
  public void testIdsQuery() {
    QueryBuilder qb = QueryBuilders.idsQuery().ids("1", "2");

    SearchResponse response = srb.setQuery(qb).execute().actionGet();

    // read response
    printTotalAndFirstPage(response);
  }

  @Test
  public void testConstantScoreQuery() {
    // Using with Filters
    QueryBuilder qb1 =
        QueryBuilders.constantScoreQuery(FilterBuilders.termFilter(fieldShakeSpeare, "henry"))
            .boost(2.0f);

    // With Queries
    QueryBuilder qb2 =
        QueryBuilders.constantScoreQuery(QueryBuilders.termQuery(fieldShakeSpeare, "henry")).boost(
            2.0f);

    SearchResponse response1 = srb.setQuery(qb1).execute().actionGet();
    SearchResponse response2 = srb.setQuery(qb2).execute().actionGet();

    // read response
    long totalMatched = response1.getHits().getTotalHits();
    System.out.println("Total matched by filter query: " + totalMatched);

    totalMatched = response2.getHits().getTotalHits();
    System.out.println("Total matched by pure query: " + totalMatched);
  }

  @Test
  public void testDisjunctionMaxQuery() {
    // corresponding to best fields strategy
    // http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/_best_fields.html
    QueryBuilder qb =
        QueryBuilders.disMaxQuery().add(QueryBuilders.termQuery(fieldShakeSpeare, "henry"))
            .add(QueryBuilders.termQuery("speaker", "henry")).boost(1.2f).tieBreaker(0.7f);

    SearchResponse response = srb.setQuery(qb).execute().actionGet();

    // read response
    printTotalAndFirstPage(response);
  }

  @Test
  public void testFuzzyLikeThis() {
    // fuzzy like this can specify more than one fields
    // http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-flt-query.html
    // http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-flt-field-query.html
    QueryBuilder qb =
        QueryBuilders.fuzzyLikeThisQuery(fieldShakeSpeare, "play_name")
            .likeText("havoc and confusion").maxQueryTerms(12);

    SearchResponse response = srb.setQuery(qb).execute().actionGet();

    // read response
    printTotalAndFirstPage(response);
  }

  @Test
  public void testFuzzyQuery() {
    QueryBuilder qb = QueryBuilders.fuzzyQuery(fieldShakeSpeare, "havo");

    SearchResponse response = srb.setQuery(qb).execute().actionGet();

    // read response
    printTotalAndFirstPage(response);
  }

  @Test
  public void testHasChildQuery() {
    // http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-has-child-query.html
    // TODO
  }

  @Test
  public void testHasParentQuery() {
    // http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-has-parent-query.html
    // TODO
  }

  @Test
  public void testMatchAllQuery() {
    // A query that matches all documents
    QueryBuilder qb = QueryBuilders.matchAllQuery();

    SearchResponse response = srb.setQuery(qb).execute().actionGet();

    // read response
    printTotalAndFirstPage(response);
  }

  @Test
  public void testMoreLikeThis() {
    // http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-mlt-query.html
    // http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-mlt-field-query.html
    // TODO
  }

  @Test
  public void testPrefixQuery() {
    // http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-prefix-query.html
    QueryBuilder qb = QueryBuilders.prefixQuery(fieldShakeSpeare, "again");

    SearchResponse response = srb.setQuery(qb).execute().actionGet();

    // read response
    printTotalAndFirstPage(response);
  }

  @Test
  public void testQuerystringQuery() {
    // http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html
    QueryBuilder qb = QueryBuilders.queryString("+havoc");

    SearchResponse response = srb.setQuery(qb).execute().actionGet();

    // read response
    printTotalAndFirstPage(response);
  }

  @Test
  public void testRangeQuery() {
    QueryBuilder qb =
        QueryBuilders.rangeQuery("speech_number").from(5).to(10).includeLower(true)
            .includeUpper(false);

    SearchResponse response = srb.setQuery(qb).execute().actionGet();

    // read response
    printTotalAndFirstPage(response);
  }

  @Test
  public void testSpanQueries() {
    // http://www.elasticsearch.org/guide/en/elasticsearch/client/java-api/current/query-dsl-queries.html#_span_queries_first_near_not_or_term
    // TODO
  }

  @Test
  public void testTermQuery() {
    QueryBuilder qb = QueryBuilders.termQuery(fieldShakeSpeare, "havoc");

    SearchResponse response = srb.setQuery(qb).execute().actionGet();

    // read response
    printTotalAndFirstPage(response);
  }

  @Test
  public void testTermsQuery() {
    QueryBuilder qb =
        QueryBuilders.termsQuery(fieldShakeSpeare, "havoc", "elephant").minimumMatch(1);

    SearchResponse response = srb.setQuery(qb).execute().actionGet();

    // read response
    printTotalAndFirstPage(response);
  }

  @Test
  public void testWildcardQuery() {
    QueryBuilder qb = QueryBuilders.wildcardQuery(fieldShakeSpeare, "ha*c");

    SearchResponse response = srb.setQuery(qb).execute().actionGet();

    // read response
    printTotalAndFirstPage(response);
  }

  @Test
  public void testTopChildrenQuery() {
    // http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-top-children-query.html
    // TODO
  }

  @Test
  public void testNestedQuery() {
    // http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-nested-query.html
    // TODO
  }

  @Test
  public void testIndicesQuery() {
    // http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-indices-query.html
    // TODO
  }

  @Test
  public void testGeoShapeQuery() {
    // http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-geo-shape-query.html
    // TODO
  }

  private void printTotalAndFirstPage(SearchResponse response) {
    long totalMatched = response.getHits().getTotalHits();
    System.out.println("Total matched: " + totalMatched);

    // list score and content of each hit
    Arrays.asList(response.getHits().getHits()).forEach(
        hit -> {
          System.out.println(String.format("Score: %f, Content: %s", hit.getScore(), hit
              .getSource().get(fieldShakeSpeare)));
        });
  }

}
