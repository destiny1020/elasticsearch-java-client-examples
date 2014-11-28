package es.official.guide.agg;

import java.util.LinkedList;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram.Interval;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.junit.Test;

import es.ESTestBase;

public class DateHistogramExamples extends ESTestBase {

  private SearchRequestBuilder srb;

  //  # Find the average price of each month's top-selling car type
  //  GET /cars/transactions/_search?search_type=count
  //  {
  //     "aggs": {
  //        "sales": {
  //           "date_histogram": {
  //              "field": "sold",
  //              "interval": "month",
  //              "format": "yyyy-MM-dd",
  //              "min_doc_count" : 0,
  //              "extended_bounds" : {
  //                  "min" : "2014-01-01",
  //                  "max" : "2014-12-31"
  //              }
  //           },
  //           "aggs": {
  //              "top_selling": {
  //                 "terms": {
  //                    "field": "make",
  //                    "size": 1
  //                 },
  //                 "aggs": {
  //                    "avg_price": {
  //                       "avg": { "field": "price" }
  //                    }
  //                 }
  //              }
  //           }
  //        }
  //     }
  //  }
  @Test
  public void testExtendedDateHistogram() {
    String sales = "sales", topSelling = "topSelling", avgPrice = "avg_price";

    srb =
        client
            .prepareSearch("cars")
            .setSearchType(SearchType.COUNT)
            .addAggregation(
                AggregationBuilders
                    .dateHistogram("sales")
                    .field("sold")
                    .interval(Interval.MONTH)
                    .format("yyyy-MM-dd")
                    .minDocCount(0)
                    .extendedBounds("2014-01-01", "2014-12-31")
                    .subAggregation(
                        AggregationBuilders.terms(topSelling).field("make").size(1)
                            .subAggregation(AggregationBuilders.avg(avgPrice).field("price"))));

    SearchResponse response = srb.execute().actionGet();

    // read agg
    DateHistogram dateHistogram = (DateHistogram) response.getAggregations().get(sales);
    dateHistogram
        .getBuckets()
        .forEach(bucket -> {
          String keyString = bucket.getKeyAsText().string();
          long docCount = bucket.getDocCount();
          Terms topSellingTerms = (Terms) bucket.getAggregations().get(topSelling);

          // TODO: find better approach to get the top bucket, since size is 1
            if (topSellingTerms.getBuckets().size() > 0) {
              Bucket topBucket = new LinkedList<>(topSellingTerms.getBuckets()).get(0);
              String topSellingKey = topBucket.getKey();
              long topSellingDocCount = topBucket.getDocCount();
              double avgPriceNumber = ((Avg) topBucket.getAggregations().get(avgPrice)).getValue();
              // print info
              System.out.println(String
                  .format(
                      "Time period: %s, Doc count: %d [Top selling make: %s, number: %d, Average price: %f]",
                      keyString, docCount, topSellingKey, topSellingDocCount, avgPriceNumber));
            }
          });
  }
}
