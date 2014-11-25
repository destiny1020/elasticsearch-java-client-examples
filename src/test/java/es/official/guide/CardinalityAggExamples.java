package es.official.guide;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram.Interval;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.junit.Test;

import es.ESTestBase;

/**
 * http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/cardinality.html
 */
public class CardinalityAggExamples extends ESTestBase {

  //  GET /cars/transactions/_search?search_type=count
  //  {
  //      "aggs" : {
  //          "distinct_colors" : {
  //              "cardinality" : {
  //                "field" : "color"
  //              }
  //          }
  //      }
  //  }
  @Test
  public void testCardinality() {
    String distinctColors = "distinct_colors";

    SearchResponse response =
        client.prepareSearch("cars")
            .addAggregation(AggregationBuilders.cardinality(distinctColors).field("color"))
            .execute().actionGet();

    // read agg
    Cardinality cardinality = (Cardinality) response.getAggregations().get(distinctColors);
    System.out.println("Cardinality for colors: " + cardinality.getValue());
  }

  // # How many colors were sold each month ?
  //  GET /cars/transactions/_search?search_type=count
  //  {
  //    "aggs" : {
  //        "months" : {
  //          "date_histogram": {
  //            "field": "sold",
  //            "interval": "month"
  //          },
  //          "aggs": {
  //            "distinct_colors" : {
  //                "cardinality" : {
  //                  "field" : "color"
  //                }
  //            }
  //          }
  //        }
  //    }
  //  }
  @Test
  public void testCardinalityWithDateHistogram() {
    String months = "months", distinctColors = "distinct_colors";

    SearchResponse response =
        client
            .prepareSearch("cars")
            .addAggregation(
                AggregationBuilders.dateHistogram(months).field("sold").interval(Interval.MONTH)
                    .subAggregation(AggregationBuilders.cardinality(distinctColors).field("color")
                    // control the precision
                        .precisionThreshold(100))).execute().actionGet();

    // read response
    DateHistogram dateHistogram = (DateHistogram) response.getAggregations().get(months);
    dateHistogram.getBuckets().forEach(
        bucket -> {
          Cardinality card = bucket.getAggregations().get(distinctColors);
          System.out.println(String.format("Date: %s ---> Sold Colors: %d", bucket.getKeyAsText()
              .string(), card.getValue()));
        });
  }

}
