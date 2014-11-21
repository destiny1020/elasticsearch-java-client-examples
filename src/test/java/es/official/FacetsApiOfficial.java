package es.official;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.filter.FilterFacet;
import org.elasticsearch.search.facet.filter.FilterFacetBuilder;
import org.elasticsearch.search.facet.histogram.HistogramFacet;
import org.elasticsearch.search.facet.histogram.HistogramFacetBuilder;
import org.elasticsearch.search.facet.query.QueryFacet;
import org.elasticsearch.search.facet.query.QueryFacetBuilder;
import org.elasticsearch.search.facet.range.RangeFacet;
import org.elasticsearch.search.facet.range.RangeFacetBuilder;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.junit.Test;

import es.ESTestBase;

public class FacetsApiOfficial extends ESTestBase {

  private String facetName = "source";
  private String rangeFacetName = "rangeFacet";
  private String histogramFacetName = "histogramFacet";
  private String filterFacetName = "filterFacet";
  private String queryFacetName = "queryFacet";

  private String targetText = "henry";

  @Test
  public void testFacets() {
    // obtain all line contains "henry"
    SearchResponse response =
        client.prepareSearch(indexShakeSpeare)
            .setQuery(QueryBuilders.termQuery(fieldShakeSpeare, targetText))
            .addFacet(FacetBuilders.termsFacet(facetName).field("speaker").size(20)).execute()
            .actionGet();

    // read response
    TermsFacet f = (TermsFacet) response.getFacets().facetsAsMap().get(facetName);

    // Total terms doc count
    System.out.println(String.format("Total terms doc count: %d", f.getTotalCount()));

    // Not shown terms doc count
    System.out.println(String.format("Not shown terms doc count: %d", f.getOtherCount()));

    // Without term doc count
    System.out.println(String.format("Without term doc count: %d", f.getMissingCount()));

    // For each entry
    for (TermsFacet.Entry entry : f) {
      System.out.println(String.format("Term: %s", entry.getTerm()));
      System.out.println(String.format("Doc count: %d", entry.getCount()));
    }
  }

  @Test
  public void testRangeFacet() {
    RangeFacetBuilder rfb =
        FacetBuilders.rangeFacet(rangeFacetName).field("speech_number").addRange(2, 3);

    // more examples:
    // FacetBuilders.rangeFacet("f")
    // .field("price") // Field to compute on
    // .addUnboundedFrom(3) // from -infinity to 3 (excluded)
    // .addRange(3, 6) // from 3 to 6 (excluded)
    // .addUnboundedTo(6); // from 6 to +infinity

    SearchResponse response =
        client.prepareSearch(indexShakeSpeare)
            .setQuery(QueryBuilders.termQuery(fieldShakeSpeare, targetText)).addFacet(rfb)
            .execute().actionGet();

    RangeFacet f = (RangeFacet) response.getFacets().facetsAsMap().get(rangeFacetName);

    // For each entry
    for (RangeFacet.Entry entry : f) {
      // Range from requested
      System.out.println(String.format("Range from requested: %f", entry.getFrom()));

      // Range to requested
      System.out.println(String.format("Range to requested: %f", entry.getTo()));

      // Doc count
      System.out.println(String.format("Doc count: %d", entry.getCount()));

      // Min value
      System.out.println(String.format("Min value: %f", entry.getMin()));

      // Max value
      System.out.println(String.format("Max value: %f", entry.getMax()));

      // Mean value
      System.out.println(String.format("Mean value: %f", entry.getMean()));

      // Sum of values
      System.out.println(String.format("Sum of values: %f", entry.getTotal()));
    }
  }

  @Test
  public void testHistogramFacet() {
    HistogramFacetBuilder facet =
        FacetBuilders.histogramFacet(histogramFacetName).field("speech_number").interval(1);

    SearchResponse response =
        client.prepareSearch(indexShakeSpeare)
            .setQuery(QueryBuilders.termQuery(fieldShakeSpeare, targetText)).addFacet(facet)
            .execute().actionGet();

    HistogramFacet f = (HistogramFacet) response.getFacets().facetsAsMap().get(histogramFacetName);

    // For each entry
    for (HistogramFacet.Entry entry : f) {
      // Key (X-Axis)
      System.out.println(String.format("Key (X-Axis): %d", entry.getKey()));

      // Doc count (Y-Axis)
      System.out.println(String.format("Doc count (Y-Axis): %d", entry.getCount()));;
    }
  }

  @Test
  public void testDateHistogramFacet() {
    // TODO

    // FacetBuilders.dateHistogramFacet("f").field("date") // Your date field
    // .interval("year"); // You can also use "quarter", "month", "week", "day",
    // // "hour" and "minute" or notation like "1.5h" or "2w"

    // // sr is here your SearchResponse object
    // DateHistogramFacet f = (DateHistogramFacet) sr.getFacets().facetsAsMap().get("f");
    //
    // // For each entry
    // for (DateHistogramFacet.Entry entry : f) {
    // entry.getTime(); // Date in ms since epoch (X-Axis)
    // entry.getCount(); // Doc count (Y-Axis)
    // }
  }

  @Test
  public void testFilterFacet() {
    FilterFacetBuilder filterFacet =
        FacetBuilders
            .filterFacet(filterFacetName, FilterBuilders.termFilter("speaker", "MORTIMER"));

    SearchResponse response =
        client.prepareSearch(indexShakeSpeare)
            .setQuery(QueryBuilders.termQuery(fieldShakeSpeare, targetText)).addFacet(filterFacet)
            .execute().actionGet();

    FilterFacet f = (FilterFacet) response.getFacets().facetsAsMap().get(filterFacetName);

    // Number of docs that matched
    System.out.println(String.format("Number of docs that matched: %d", f.getCount()));
  }

  @Test
  public void testQueryFacet() {
    MatchQueryBuilder matchQuery = QueryBuilders.matchQuery(fieldShakeSpeare, "havoc");

    QueryFacetBuilder queryFacet = FacetBuilders.queryFacet(queryFacetName, matchQuery);

    // no need to add query since facet itself is a query
    SearchResponse response =
        client.prepareSearch(indexShakeSpeare).addFacet(queryFacet).execute().actionGet();

    QueryFacet f = (QueryFacet) response.getFacets().facetsAsMap().get(queryFacetName);

    // Number of docs that matched
    System.out.println(String.format("Number of docs that matched: %d", f.getCount()));
  }

  @Test
  public void testStatistical() {
    // TODO
  }

  @Test
  public void testTermStatsFacet() {
    // TODO
  }

  // not filter facet as above
  @Test
  public void testFacetFilter() {
    // TODO
  }

  @Test
  public void testScopeGlobal() {

  }
}
