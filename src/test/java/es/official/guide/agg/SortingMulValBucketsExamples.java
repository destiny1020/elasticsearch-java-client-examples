package es.official.guide.agg;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.junit.Test;

import es.ESTestBase;

/**
 * http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/_sorting_multi_value_buckets.html
 */
public class SortingMulValBucketsExamples extends ESTestBase {

  private SearchRequestBuilder srb;

  //  # Sort colors by count ascending
  //  GET /cars/transactions/_search?search_type=count
  //  {
  //      "aggs" : {
  //          "colors" : {
  //              "terms" : {
  //                "field" : "color",
  //                "order": {
  //                  "_count" : "asc"
  //                }
  //              }
  //          }
  //      }
  //  }
  @Test
  public void testSortAsc() {
    srb =
        client.prepareSearch("cars").addAggregation(
            AggregationBuilders.terms("colors").field("color").order(Order.count(true)));

    SearchResponse response = srb.execute().actionGet();

    // read agg
    Terms colorsTerms = (Terms) response.getAggregations().get("colors");
    colorsTerms.getBuckets().forEach(
        bucket -> {
          System.out.println(String.format("Key: %s, Doc count: %d", bucket.getKey(),
              bucket.getDocCount()));
        });
  }

  //  # Sort colors by average price of cars in that color (using metric as a sort value)
  //  GET /cars/transactions/_search?search_type=count
  //  {
  //      "aggs" : {
  //          "colors" : {
  //              "terms" : {
  //                "field" : "color",
  //                "order": {
  //                  "avg_price" : "asc"
  //                }
  //              },
  //              "aggs": {
  //                  "avg_price": {
  //                      "avg": {"field": "price"}
  //                  }
  //              }
  //          }
  //      }
  //  }
  @Test
  public void testSortingByMetric() {
    String termsColors = "colors", avgPrice = "avg_price";
    srb =
        client.prepareSearch("cars").addAggregation(
            AggregationBuilders.terms(termsColors).field("color")
                .order(Order.aggregation(avgPrice, true))
                .subAggregation(AggregationBuilders.avg(avgPrice).field("price")));

    SearchResponse response = srb.execute().actionGet();

    // read agg
    Terms colorsTerms = (Terms) response.getAggregations().get("colors");
    colorsTerms.getBuckets().forEach(
        bucket -> {
          double averagePrice = ((Avg) bucket.getAggregations().get(avgPrice)).getValue();
          System.out.println(String.format("Key: %s, Doc count: %d, Average Price: %f",
              bucket.getKey(), bucket.getDocCount(), averagePrice));
        });
  }

  //  # Similar to above, but sorting on a multi-valued metric
  //  GET /cars/transactions/_search?search_type=count
  //  {
  //      "aggs" : {
  //          "colors" : {
  //              "terms" : {
  //                "field" : "color",
  //                "order": {
  //                  "stats.variance" : "asc"
  //                }
  //              },
  //              "aggs": {
  //                  "stats": {
  //                      "extended_stats": {"field": "price"}
  //                  }
  //              }
  //          }
  //      }
  //  }
  @Test
  public void testSortingByMulValMetric() {
    String termsColors = "colors", stats = "stats";
    srb =
        client.prepareSearch("cars").addAggregation(
            AggregationBuilders.terms(termsColors).field("color")
                .order(Order.aggregation("stats.variance", true))
                .subAggregation(AggregationBuilders.extendedStats(stats).field("price")));

    SearchResponse response = srb.execute().actionGet();

    // read agg
    Terms colorsTerms = (Terms) response.getAggregations().get("colors");
    colorsTerms.getBuckets().forEach(
        bucket -> {
          double variancePrice =
              ((ExtendedStats) bucket.getAggregations().get(stats)).getVariance();
          System.out.println(String.format("Key: %s, Doc count: %d, Variance: %f", bucket.getKey(),
              bucket.getDocCount(), variancePrice));
        });
  }

  //  # Sorting on a "grandchild" nested metric
  //  GET /cars/transactions/_search?search_type=count
  //  {
  //      "aggs" : {
  //          "colors" : {
  //              "histogram" : {
  //                "field" : "price",
  //                "interval": 20000,
  //                "order": {
  //                  "red_green_cars>stats.variance" : "asc"
  //                }
  //              },
  //              "aggs": {
  //                  "red_green_cars": { 
  //                      "filter": { "terms": {"color": ["red", "green"]}},
  //                      "aggs": {
  //                          "stats": {"extended_stats": {"field" : "price"}}
  //                      }
  //                  }
  //              }
  //          }
  //      }
  //  }
  @Test
  public void testSortingOnNestedMetric() {
    String colors = "colors", red_green_cars = "red_green_cars", stats = "stats";

    srb =
        client.prepareSearch("cars").addAggregation(
            AggregationBuilders
                .histogram(colors)
                .field("price")
                .interval(20000)
                .order(
                    org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Order
                        .aggregation("red_green_cars>stats.variance", true))
                .subAggregation(
                    AggregationBuilders.filter(red_green_cars)
                        .filter(FilterBuilders.termsFilter("color", "red", "green"))
                        .subAggregation(AggregationBuilders.extendedStats(stats).field("price"))));

    SearchResponse response = srb.execute().actionGet();

    // read agg
    Histogram histogram = (Histogram) response.getAggregations().get(colors);
    histogram.getBuckets().forEach(
        bucket -> {
          Filter filterBucket = (Filter) bucket.getAggregations().get(red_green_cars);
          ExtendedStats estats = (ExtendedStats) filterBucket.getAggregations().get(stats);
          double variance = estats.getVariance();

          System.out.println(String.format("Key: %s, Doc count: %d, Variance: %f", bucket.getKey(),
              bucket.getDocCount(), variance));
        });
  }
}
