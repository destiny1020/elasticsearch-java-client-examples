package es.official.guide;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentileRanks;
import org.junit.Test;

import es.ESTestBase;

public class PercentileRanksAggExamples extends ESTestBase {

  //  GET /website/logs/_search?search_type=count
  //  {
  //      "aggs" : {
  //          "zones" : {
  //              "terms" : {
  //                  "field" : "zone"
  //              },
  //              "aggs" : {
  //                  "load_times" : {
  //                      "percentile_ranks" : {
  //                        "field" : "latency",
  //                        "values" : [210, 800] 
  //                      }
  //                  }
  //              }
  //          }
  //      }
  //  }
  @Test
  public void testPercentileRanks() {
    String zones = "zones", loadTimes = "load_times";

    SearchResponse response =
        client
            .prepareSearch("website")
            .setTypes("logs")
            .setSearchType(SearchType.COUNT)
            .addAggregation(
                AggregationBuilders
                    .terms(zones)
                    .field("zone")
                    .subAggregation(
                        AggregationBuilders.percentileRanks(loadTimes).field("latency")
                            .percentiles(210, 800))).execute().actionGet();

    // read agg
    Terms terms = response.getAggregations().get(zones);
    terms.getBuckets().forEach(
        bucket -> {
          PercentileRanks pr = bucket.getAggregations().get(loadTimes);
          pr.forEach((Percentile percentile) -> {
            System.out.println(String.format("Zone: %s, Latency %f at %f%%", bucket.getKey(),
                percentile.getValue(), percentile.getPercent()));
          });
        });
  }
}
