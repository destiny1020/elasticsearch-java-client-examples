package es.official.guide.sid;

import java.io.IOException;
import java.util.Arrays;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;

import es.ESTestBase;

/**
 * http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/structured-search.html
 */
public class StructuredSearchExamples extends ESTestBase {

  private String indexName = "my_store";
  private String typeName = "products";

  @Test
  public void prepareIndexAndData() throws IOException {
    IndicesExistsResponse indicesExistsResponse =
        client.admin().indices().prepareExists(indexName).execute().actionGet();

    if (indicesExistsResponse.isExists()) {
      DeleteIndexResponse deleteIndexResponse =
          client.admin().indices().prepareDelete(indexName).execute().actionGet();

      if (deleteIndexResponse.isAcknowledged()) {
        System.out.println("Delete index completed.");
      } else {
        System.err.println("Delte index failed.");
      }
      
      // create index first
      CreateIndexResponse cir = client.admin().indices().prepareCreate(indexName).setSettings(XContentFactory.jsonBuilder()
          .startObject()
            .field("number_of_shards", 1)
            .field("number_of_replicas", 0)
          .endObject()).execute().actionGet();
      
      if(cir.isAcknowledged()) {
        System.out.println("Index created.");
      } else {
        System.err.println("Index creation failed.");
        return;
      }
      
      // create the mapping for type products
      PutMappingResponse pmr = client.admin().indices().preparePutMapping(indexName).setType(typeName).setSource(XContentFactory.jsonBuilder()
          .startObject()
            .startObject(typeName)
              .startObject("properties")
                .startObject("productID")
                  .field("type", "string")
                  .field("index", "not_analyzed")
                .endObject()
              .endObject()
            .endObject()
          .endObject()).execute().actionGet();
      
      if(pmr.isAcknowledged()) {
        System.out.println("Mapping created.");
      } else {
        System.err.println("Mapping creation failed.");
      }
    } else {
      System.out.println("No need to delete index since it is not existing.");
    }
    
    // create data in bulk
    BulkRequestBuilder brb = client.prepareBulk().add(client.prepareIndex(indexName, typeName, "1").setSource(XContentFactory.jsonBuilder()
        .startObject()
          .field("price", 10)
          .field("productID", "XHDK-A-1293-#fJ3")
        .endObject()));
    
    brb.add(client.prepareIndex(indexName, typeName, "2").setSource(XContentFactory.jsonBuilder()
        .startObject()
          .field("price", 20)
          .field("productID", "KDKE-B-9947-#kL5")
        .endObject()));
    
    brb.add(client.prepareIndex(indexName, typeName, "3").setSource(XContentFactory.jsonBuilder()
        .startObject()
          .field("price", 30)
          .field("productID", "JODL-X-1937-#pV7")
        .endObject()));
    brb.add(client.prepareIndex(indexName, typeName, "4").setSource(XContentFactory.jsonBuilder()
        .startObject()
          .field("price", 30)
          .field("productID", "QQPX-R-3956-#aD8")
        .endObject()));

    BulkResponse bulkResponse = brb.execute().actionGet();
    
    if(bulkResponse.hasFailures()) {
      System.err.println(bulkResponse.buildFailureMessage());
    } else {
      System.out.println("Bluk indexing completed.");
    }
  }
  
//  GET /my_store/products/_search
//  {
//      "query" : {
//          "filtered" : { 
//              "query" : {
//                  "match_all" : {} 
//              },
//              "filter" : {
//                  "term" : { 
//                      "price" : 20
//                  }
//              }
//          }
//      }
//  }
  @Test
  public void testFilteredQuery() {
    SearchResponse response = client.prepareSearch(indexName).setTypes(typeName).setQuery(QueryBuilders.filteredQuery(
        QueryBuilders.matchAllQuery(), 
        FilterBuilders.termFilter("price", 20))).execute().actionGet();
    
    System.out.println(response);
    
    // filter on productID, which is a not_analyzed string field
    response = client.prepareSearch(indexName).setTypes(typeName).setQuery(QueryBuilders.filteredQuery(
        QueryBuilders.matchAllQuery(), 
        FilterBuilders.termFilter("productID", "XHDK-A-1293-#fJ3"))).execute().actionGet();
    
    System.out.println(response);
  }
  
//  SELECT product
//  FROM   products
//  WHERE  (price = 20 OR productID = "XHDK-A-1293-#fJ3")
//    AND  (price != 30)
  @Test
  public void testBoolFilter() {
    SearchResponse response = client.prepareSearch(indexName).setTypes(typeName).setQuery(QueryBuilders.filteredQuery(
        QueryBuilders.matchAllQuery(),
        FilterBuilders.boolFilter()
          .should(FilterBuilders.termFilter("price", 20),
            FilterBuilders.termFilter("productID", "XHDK-A-1293-#fJ3"))
          .mustNot(FilterBuilders.termFilter("price", 30)))).execute().actionGet();
    
    System.out.println(response);
  }
  
//  SELECT document
//  FROM   products
//  WHERE  productID      = "KDKE-B-9947-#kL5"
//    OR (     productID = "JODL-X-1937-#pV7"
//         AND price     = 30 )
  @Test
  public void testNestedBoolFilter() {
    SearchResponse response = client.prepareSearch(indexName).setTypes(typeName).setQuery(QueryBuilders.filteredQuery(
        QueryBuilders.matchAllQuery(), 
        FilterBuilders.boolFilter().should(
            FilterBuilders.termFilter("productID", "KDKE-B-9947-#kL5"),
            FilterBuilders.boolFilter().must(
                FilterBuilders.termFilter("productID", "JODL-X-1937-#pV7"),
                FilterBuilders.termFilter("price", 30)
            )))).execute().actionGet();
    
    System.out.println(response);
  }
  
  @Test
  public void testFindMultipleExactValues() {
    SearchResponse response = client.prepareSearch(indexName).setTypes(typeName).setQuery(QueryBuilders.filteredQuery(
        QueryBuilders.matchAllQuery(),
        FilterBuilders.termsFilter("price", Arrays.asList(20, 30)))).execute().actionGet();
    
    System.out.println(response);
  }
  
//  SELECT document
//  FROM   products
//  WHERE  price BETWEEN 20 AND 40
  @Test
  public void testFindRange() {
    SearchResponse response = client.prepareSearch(indexName).setTypes(typeName).setQuery(QueryBuilders.filteredQuery(
        QueryBuilders.matchAllQuery(),
        FilterBuilders.rangeFilter("price")
          .gt(20).lt(40))).execute().actionGet();
    
    System.out.println(response);
  }

}
