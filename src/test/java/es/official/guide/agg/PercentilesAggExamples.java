package es.official.guide.agg;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.junit.Test;

import es.ESTestBase;

/**
 * http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/percentiles.html
 */
public class PercentilesAggExamples extends ESTestBase {

  //  GET /website/logs/_search?search_type=count
  //  {
  //      "aggs" : {
  //          "load_times" : {
  //              "percentiles" : {
  //                  "field" : "latency" 
  //              }
  //          },
  //          "avg_load_time" : {
  //              "avg" : {
  //                  "field" : "latency" 
  //              }
  //          }
  //      }
  //  }
  @Test
  public void testPercentiles() {
    String loadTimes = "load_times", avgLoadTime = "avg_load_time";

    SearchResponse response =
        client.prepareSearch("website").setTypes("logs")
            .addAggregation(AggregationBuilders.percentiles(loadTimes).field("latency"))
            .addAggregation(AggregationBuilders.avg(avgLoadTime).field("latency")).execute()
            .actionGet();

    // read agg
    Percentiles percentiles = response.getAggregations().get(loadTimes);
    percentiles.forEach(percentile -> {
      System.out.printf("Percentage: %f, Value: %f\n", percentile.getPercent(),
          percentile.getValue());
    });

    Avg avg = response.getAggregations().get(avgLoadTime);
    System.out.println("Average latency: " + avg.getValue());
  }

  //  GET /website/logs/_search?search_type=count
  //  {
  //      "aggs" : {
  //          "zones" : {
  //              "terms" : {
  //                  "field" : "zone" 
  //              },
  //              "aggs" : {
  //                  "load_times" : {
  //                      "percentiles" : { 
  //                        "field" : "latency",
  //                        "percents" : [50, 95.0, 99.0] 
  //                      }
  //                  },
  //                  "load_avg" : {
  //                      "avg" : {
  //                          "field" : "latency"
  //                      }
  //                  }
  //              }
  //          }
  //      }
  //  }
  @Test
  public void testPercentilesWithTerms() {
    String zones = "zones", loadTimes = "load_times", avgLoadTime = "avg_load_time";

    SearchResponse response =
        client
            .prepareSearch("website")
            .setTypes("logs")
            .addAggregation(
                AggregationBuilders
                    .terms(zones)
                    .field("zone")
                    .subAggregation(
                        AggregationBuilders.percentiles(loadTimes).field("latency")
                            .percentiles(50.0, 95.0, 99.0))
                    .subAggregation(AggregationBuilders.avg(avgLoadTime).field("latency")))
            .execute().actionGet();

    // read agg
    Terms terms = response.getAggregations().get(zones);
    terms.getBuckets().forEach(
        bucket -> {
          Percentiles percentiles = bucket.getAggregations().get(loadTimes);
          percentiles.forEach(percentile -> {
            System.out.printf("Percentage: %f, Value: %f\n", percentile.getPercent(),
                percentile.getValue());
          });

          Avg avg = bucket.getAggregations().get(avgLoadTime);
          System.out.println("Average latency: " + avg.getValue());
        });

  }
}
