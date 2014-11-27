package es.official.guide.modeling;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram.Interval;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

import es.ESTestBase;

/**
 * http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/nested-mapping.html 
 * http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/nested-query.html
 * http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/nested-sorting.html
 */
public class NestedObjectMappingExamples extends ESTestBase {

  private String indexName = "nested";
  private String typeName = "blogpost";

  @Test
  public void testCreateIndex() throws IOException {
    CreateIndexRequestBuilder cirb = client.admin().indices().prepareCreate(indexName).setSource(XContentFactory.jsonBuilder()
        .startObject()
          .startObject("settings")
            .field("number_of_shards", 1)
            .field("number_of_replicas", 0)
          .endObject()
        .endObject());
    
    CreateIndexResponse response = cirb.execute().actionGet();
    if(response.isAcknowledged()) {
      System.out.println("Index created.");
    } else {
      System.err.println("Index creation failed.");
    }
  }
  
  @Test
  public void testCreateNestedMapping() throws IOException {
    
    // if the setType is used, then the startObject("mapping") is not necessary
    PutMappingResponse response = client.admin().indices().preparePutMapping(indexName).setType(typeName).setSource(XContentFactory.jsonBuilder()
        .startObject()
//          .startObject("mappings")
            .startObject(typeName)
              .startObject("properties")
                .startObject("comments")
                  .field("type", "nested")
                  .startObject("properties")
                    // field name
                    .startObject("name")
                      .field("type", "string")
                    .endObject()
                    // field comment
                    .startObject("comment")
                      .field("type", "string")
                    .endObject()
                    // field age
                    .startObject("age")
                      .field("type", "short")
                    .endObject()
                    // field stars
                    .startObject("stars")
                      .field("type", "short")
                    .endObject()
                    // field date
                    .startObject("date")
                      .field("type", "date")
                    .endObject()
                  .endObject()
                .endObject()
              .endObject()
            .endObject()
//          .endObject()
        .endObject()).execute().actionGet();
    
    // read mapping response
    if(response.isAcknowledged()) {
      System.out.println("Create mapping with nested type succeeded.");
    } else {
      System.err.println("Create mapping with nested type failed.");
    }
  }
  
  @Test
  public void testIndexNestedDocument() throws IOException {
    // creating json contained array:
    // http://stackoverflow.com/questions/10170053/how-to-index-an-array-of-nested-type-in-elasticsearch
    IndexRequestBuilder irb = client.prepareIndex(indexName, typeName, "1").setSource(XContentFactory.jsonBuilder()
        .startObject()
          .field("title", "Nest eggs")
          .field("body", "Making your money work...")
          .field("tags", "cash", "shares")
          .startArray("comments")
            // 1st comment
            .startObject()
              .field("name", "John Smith")
              .field("comment", "Great article")
              .field("age", 28)
              .field("stars", 4)
              .field("date", "2014-09-01")
            .endObject()
            // 2nd comment
            .startObject()
              .field("name", "Alice White")
              .field("comment", "More like this please")
              .field("age", 31)
              .field("stars", 5)
              .field("date", "2014-10-22")
            .endObject()
          .endArray()
        .endObject());
    
    IndexResponse response = irb.execute().actionGet();
    if(response.isCreated()) {
      System.out.println("Creating nested document succeeded.");
    } else {
      System.err.println("Creating nested document failed.");
    }
  }
  
  @Test
  public void testQueryNestedDocument() {
    SearchResponse response = client.prepareSearch(indexName).setTypes(typeName).setQuery(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("title", "eggs"))
        .must(QueryBuilders.nestedQuery("comments", QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("comments.name", "john"))
            .must(QueryBuilders.matchQuery("comments.age", 28))))).execute().actionGet();
    
    // read response
    SearchHit[] hits = response.getHits().getHits();
    Arrays.asList(hits).forEach(hit -> {
      Map<String, Object> source = hit.getSource();
      Object object = source.get("comments");
      System.out.println(object);
      // java.util.ArrayList
      System.out.println(object.getClass().getName());
      
      // score when the score mode is the default(average)
      System.out.println("Score by average: " + hit.getScore());
    });
  }
  
  @Test
  public void testQueryNestedDocumentWithScoreMode() {
    SearchResponse response = client.prepareSearch(indexName).setTypes(typeName).setQuery(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("title", "eggs"))
        .must(QueryBuilders.nestedQuery("comments", QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("comments.name", "john"))
            .must(QueryBuilders.matchQuery("comments.age", 28))).scoreMode("max"))).execute().actionGet();
    
   // read response
    SearchHit[] hits = response.getHits().getHits();
    Arrays.asList(hits).forEach(hit -> {
      Map<String, Object> source = hit.getSource();
      Object object = source.get("comments");
      System.out.println(object);
      
      // score when the score mode is the max
      System.out.println("Score by max: " + hit.getScore());
    });
  }
  
//  GET /nested/blogpost/_search
//  {
//    "query": {
//      "nested": { 
//        "path": "comments",
//        "filter": {
//          "range": {
//            "comments.date": {
//              "gte": "2014-10-01",
//              "lt":  "2014-11-01"
//            }
//          }
//        }
//      }
//    },
//    "sort": {
//      "comments.stars": { 
//        "order": "asc",   
//        "mode":  "min",   
//        "nested_filter": { 
//          "range": {
//            "comments.date": {
//              "gte": "2014-10-01",
//              "lt":  "2014-11-01"
//            }
//          }
//        }
//      }
//    }
//  }
  @Test
  public void testQueryWithSortingOnNested() {
    SearchResponse response = client.prepareSearch(indexName).setTypes(typeName).setQuery(
        QueryBuilders.nestedQuery("comments", FilterBuilders.rangeFilter("comments.date")
            .gte("2014-10-01").lt("2014-11-01")))
        .addSort(SortBuilders.fieldSort("comments.stars").order(SortOrder.ASC).sortMode("min")
            .setNestedFilter(FilterBuilders.rangeFilter("comments.date").gte("2014-10-01").lt("2014-11-01")))
        .execute().actionGet();
    
    // read response
    System.out.println(response);
  }
  
//  GET /nested/blogpost/_search?search_type=count
//  {
//    "aggs": {
//      "comments": { 
//        "nested": {
//          "path": "comments"
//        },
//        "aggs": {
//          "by_month": {
//            "date_histogram": { 
//              "field":    "comments.date",
//              "interval": "month",
//              "format":   "yyyy-MM"
//            },
//            "aggs": {
//              "avg_stars": {
//                "avg": { 
//                  "field": "comments.stars"
//                }
//              }
//            }
//          }
//        }
//      }
//    }
//  }
  @Test
  public void testQueryWithAggOnNested() {
    String aggOnComments = "comments", aggOnMonths = "by_month", aggOnAvgStars = "avg_stars";
    
    SearchResponse response = client.prepareSearch(indexName).setTypes(typeName).setSearchType(SearchType.COUNT)
      .addAggregation(AggregationBuilders.nested(aggOnComments).path(aggOnComments)
          .subAggregation(AggregationBuilders.dateHistogram(aggOnMonths).field("comments.date").interval(Interval.MONTH).format("yyyy-MM")
              .subAggregation(AggregationBuilders.avg(aggOnAvgStars).field("comments.stars")))
          ).execute().actionGet();
    
    // read response and agg
    System.out.println(response);
  }
  
//  GET /nested/blogpost/_search?search_type=count
//  {
//    "aggs": {
//      "comments": {
//        "nested": { 
//          "path": "comments"
//        },
//        "aggs": {
//          "age_group": {
//            "histogram": { 
//              "field":    "comments.age",
//              "interval": 10
//            },
//            "aggs": {
//              "blogposts": {
//                "reverse_nested": {}, 
//                "aggs": {
//                  "tags": {
//                    "terms": { 
//                      "field": "tags"
//                    }
//                  }
//                }
//              }
//            }
//          }
//        }
//      }
//    }
//  }
  @Test
  public void testQueryWithReverseAggOnNested() {
    String comments = "comments", ageGroup = "age_group", blogposts = "blogposts";
    
    SearchResponse response = client.prepareSearch(indexName).setTypes(typeName).setSearchType(SearchType.COUNT)
      .addAggregation(AggregationBuilders.nested(comments).path(comments)
          .subAggregation(AggregationBuilders.histogram(ageGroup).field("comments.age").interval(10)
              .subAggregation(AggregationBuilders.reverseNested(blogposts)
                  .subAggregation(AggregationBuilders.terms("tags").field("tags"))))).execute().actionGet();
    
    System.out.println(response);
  }

}
