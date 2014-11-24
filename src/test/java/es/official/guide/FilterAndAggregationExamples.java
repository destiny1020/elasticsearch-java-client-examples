package es.official.guide;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.junit.Test;

import es.ESTestBase;

/**
 * Implementations for:
 * http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/_scoping_aggregations.html
 * http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/_filtering_queries_and_aggregations.html 
 */
public class FilterAndAggregationExamples extends ESTestBase {

  private SearchRequestBuilder srb;

  //  # colors in ford
  //  GET /cars/transactions/_search?search_type=count
  //  {
  //    "query": {
  //      "match": {
  //        "make": "ford"
  //      }
  //    },
  //    "aggs": {
  //      "b_colors": {
  //        "terms": {
  //          "field": "color"
  //        }
  //      }
  //    }
  //  }
  @Test
  public void testQueryWithAgg() {
    String aggName = "b_colors";

    srb =
        client.prepareSearch("cars").setQuery(QueryBuilders.matchQuery("make", "ford"))
            .setSearchType(SearchType.COUNT);
    srb = srb.addAggregation(AggregationBuilders.terms(aggName).field("color"));

    SearchResponse response = srb.execute().actionGet();

    // read aggregation info
    Terms termAgg = (Terms) response.getAggregations().get(aggName);
    termAgg.getBuckets().forEach(
        bucket -> {
          System.out.println(String.format("Key: %s, Doc count: %d", bucket.getKey(),
              bucket.getDocCount()));
        });
  }

  //  # average price for ford and all
  //  GET /cars/transactions/_search?search_type=count
  //  {
  //    "query": {
  //      "match": {
  //        "make": "ford"
  //      }
  //    },
  //    "aggs": {
  //      "m_avg_ford": {
  //        "avg": {
  //          "field": "price"
  //        }
  //      },
  //      "all": {
  //        "global": {},
  //        "aggs": {
  //          "m_avg_all": {
  //            "avg": {
  //              "field": "price"
  //            }
  //          }
  //        }
  //      }
  //    }
  //  }
  @Test
  public void testQueryAvgAndAll() {
    String avgFord = "m_avg_ford", global = "all", avgAll = "m_avg_all";

    srb =
        client.prepareSearch("cars").setQuery(QueryBuilders.matchQuery("make", "ford"))
            .setSearchType(SearchType.COUNT);

    // avg on matched results
    srb.addAggregation(AggregationBuilders.avg(avgFord).field("price"));

    // avg on global
    srb.addAggregation(AggregationBuilders.global(global).subAggregation(
        AggregationBuilders.avg(avgAll).field("price")));

    SearchResponse response = srb.execute().actionGet();

    // read agg
    Avg fordAvg = (Avg) response.getAggregations().get(avgFord);
    Global globalAgg = (Global) response.getAggregations().get(global);
    Avg allAvg = (Avg) globalAgg.getAggregations().get(avgAll);

    System.out.println(String.format("Ford Avg: %f", fordAvg.getValue()));
    System.out.println(String.format("All Avg: %f", allAvg.getValue()));
  }

  //  # average price for all and query matched
  //  GET /cars/transactions/_search?search_type=count
  //  {
  //    "query": {
  //      "filtered": {
  //        "filter": {
  //          "range": {
  //            "price": {
  //              "gt": 10000
  //            }
  //          }
  //        }
  //      }
  //    },
  //    "aggs": {
  //      "m_avg": {
  //        "avg": {
  //          "field": "price"
  //        }
  //      },
  //      "all": {
  //        "global": {},
  //        "aggs": {
  //          "m_avg_all": {
  //            "avg": {
  //              "field": "price"
  //            }
  //          }
  //        }
  //      }
  //    }
  //  }
  @Test
  public void testFilteredQueryWithAggAndGlobalAgg() {
    String avgFiltered = "m_avg", global = "all", avgAll = "m_avg_all";

    srb =
        client
            .prepareSearch("cars")
            .setQuery(
                QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), FilterBuilders
                    .rangeFilter("price").gt(10000))).setSearchType(SearchType.COUNT);

    // agg on filtered results
    srb.addAggregation(AggregationBuilders.avg(avgFiltered).field("price"));
    // agg on global
    srb.addAggregation(AggregationBuilders.global(global).subAggregation(
        AggregationBuilders.avg(avgAll).field("price")));

    SearchResponse response = srb.execute().actionGet();

    // read agg
    Avg filteredAvg = (Avg) response.getAggregations().get(avgFiltered);
    Global globalAgg = (Global) response.getAggregations().get(global);
    Avg allAvg = (Avg) globalAgg.getAggregations().get(avgAll);

    System.out.println(String.format("Filtered Avg: %f", filteredAvg.getValue()));
    System.out.println(String.format("All Avg: %f", allAvg.getValue()));
  }

  //  # query for red cars, but agg on total sales on each make for red cars
  //  GET /cars/transactions/_search?search_type=count
  //  {
  //    "query": {
  //      "match": {
  //        "color": "red"
  //      }
  //    },
  //    "aggs": {
  //      "b_color_make": {
  //        "terms": {
  //          "field": "make"
  //        },
  //        "aggs": {
  //          "m_sum_price": {
  //            "sum": {
  //              "field": "price"
  //            }
  //          }
  //        }
  //      }
  //    }
  //  }
  @Test
  public void testQueryWithAggAndSubAgg() {
    String redMakers = "b_color_make", totalSalesRedMakers = "m_sum_price";

    srb =
        client.prepareSearch("cars").setQuery(QueryBuilders.matchQuery("color", "red"))
            .setSearchType(SearchType.COUNT);

    // add aggs
    srb.addAggregation(AggregationBuilders.terms(redMakers).field("make")
        .subAggregation(AggregationBuilders.sum(totalSalesRedMakers).field("price")));

    SearchResponse response = srb.execute().actionGet();

    // read agg
    Terms terms = (Terms) response.getAggregations().get(redMakers);
    terms.getBuckets().forEach(
        bucket -> {
          Sum sum = (Sum) bucket.getAggregations().get(totalSalesRedMakers);
          System.out.println(String.format("Maker: %s, Total Sales(RED car): %f for %d unit(s)",
              bucket.getKey(), sum.getValue(), bucket.getDocCount()));
        });
  }

  //  # query for red cars, but agg on red honda cars
  //  GET /cars/transactions/_search?search_type=count
  //  {
  //    "query": {
  //      "match": {
  //        "color": "red"
  //      }
  //    },
  //    "aggs": {
  //      "red_honda": {
  //        "filter": {
  //          "term": {
  //            "make": "honda"
  //          }
  //        },
  //        "aggs": {
  //          "m_sum_sales": {
  //            "sum": {
  //              "field": "price"
  //            }
  //          }
  //        }
  //      }
  //    }
  //  }
  @Test
  public void testAggWithFilter() {
    String redHonda = "red_honda", totalSalesRedHonda = "m_sum_sales";

    srb =
        client.prepareSearch("cars").setQuery(QueryBuilders.matchQuery("color", "red"))
            .setSearchType(SearchType.COUNT);

    // add aggs
    srb.addAggregation(AggregationBuilders.filter(redHonda)
        .filter(FilterBuilders.termFilter("make", "honda"))
        .subAggregation(AggregationBuilders.sum(totalSalesRedHonda).field("price")));

    SearchResponse response = srb.execute().actionGet();

    // read agg
    Filter terms = (Filter) response.getAggregations().get(redHonda);
    Sum sum = (Sum) terms.getAggregations().get(totalSalesRedHonda);

    System.out.println(String.format("Red Honda count: %d, Total Sales: %f", terms.getDocCount(),
        sum.getValue()));
  }

  //  # using post_filter to let query not affect the agg process
  //  GET /cars/transactions/_search?search_type=count
  //  {
  //    "query": {
  //      "match": {
  //        "make": "ford"
  //      }
  //    },
  //    "post_filter": {
  //      "term": {
  //        "color": "green"
  //      }
  //    },
  //    "aggs": {
  //      "m_sum_sales": {
  //        "sum": {
  //          "field": "price"
  //        }
  //      }
  //    }
  //  }
  @Test
  public void testPostFilter() {
    String totalFordSales = "m_sum_sales";

    srb =
        client.prepareSearch("cars").setQuery(QueryBuilders.matchQuery("make", "ford"))
            .setSearchType(SearchType.COUNT);

    // add agg
    srb.addAggregation(AggregationBuilders.sum(totalFordSales).field("price"));

    // add post filter
    srb.setPostFilter(FilterBuilders.termFilter("color", "green"));

    SearchResponse response = srb.execute().actionGet();

    // read total hits
    long totalHits = response.getHits().getTotalHits();

    // read agg
    Sum sum = (Sum) response.getAggregations().get(totalFordSales);
    System.out.println(String.format("After post filter, hit: %d. Total sales for Ford: %f",
        totalHits, sum.getValue()));
  }
}
