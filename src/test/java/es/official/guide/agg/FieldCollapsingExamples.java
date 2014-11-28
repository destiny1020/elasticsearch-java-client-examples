package es.official.guide.agg;

import java.io.IOException;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.junit.Test;

import es.ESTestBase;

/**
 * http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/top-hits.html 
 */
public class FieldCollapsingExamples extends ESTestBase {

  private String indexName = "my_index";
  private String typeBlogpost = "blogpost";
  private String typeUser = "user";

  @Test
  public void testCreateMappings() throws IOException {
    // mapping for the blogpost
    XContentBuilder builder =
        XContentFactory.jsonBuilder()
        .startObject()
          .startObject("properties")
            // field user
            .startObject("user")
              .startObject("properties")
                .startObject("name")
                  .field("type", "string")
                  .startObject("fields")
                    .startObject("raw")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                  .endObject()
                .endObject()
              .endObject()
            .endObject()
            // field title
            .startObject("title")
              .field("type", "string")
            .endObject()
            // field body
            .startObject("body")
              .field("type", "string")
            .endObject()
          .endObject()
        .endObject();
    
    PutMappingResponse response = client.admin().indices().preparePutMapping(indexName).setType(typeBlogpost)
      .setSource(builder).execute().actionGet();
    
    if (response.isAcknowledged()) {
      System.out.println("blogpost mapping created !");
    } else {
      System.err.println("blogpost mapping creation failed !");
    }
    
    // mapping for user
    builder =
        XContentFactory.jsonBuilder()
        .startObject()
          .startObject("properties")
            // field name
            .startObject("name")
              .field("type", "string")
            .endObject()
            // field email
            .startObject("email")
              .field("type", "string")
            .endObject()
            // field dob
            .startObject("dob")
              .field("type", "date")
            .endObject()
          .endObject()
        .endObject();
    
    response = client.admin().indices().preparePutMapping(indexName).setType(typeUser)
      .setSource(builder).execute().actionGet();
    
    if (response.isAcknowledged()) {
      System.out.println("user mapping created !");
    } else {
      System.err.println("user mapping creation failed !");
    }
  }
  
  @Test
  public void testBulkIndexing() throws IOException {
    BulkRequestBuilder bulkRequest = client.prepareBulk();
    
    // add users
    bulkRequest.add(client.prepareIndex(indexName, typeUser, "1").setSource(XContentFactory.jsonBuilder()
        .startObject()
          .field("name", "John Smith")
          .field("email", "john@smith.com")
          .field("dob", "1970-10-24")
        .endObject()));
    
    bulkRequest.add(client.prepareIndex(indexName, typeUser, "3").setSource(XContentFactory.jsonBuilder()
        .startObject()
          .field("name", "Alice John")
          .field("email", "alice@john.com")
          .field("dob", "1979-01-04")
        .endObject()));
    
    // add blogposts
    bulkRequest.add(client.prepareIndex(indexName, typeBlogpost, "2").setSource(XContentFactory.jsonBuilder()
        .startObject()
          .field("title", "Relationships")
          .field("body", "It's complicated...")
          .startObject("user")
            .field("id", 1)
            .field("name", "John Smith")
          .endObject()
        .endObject()));
    
    bulkRequest.add(client.prepareIndex(indexName, typeBlogpost, "4").setSource(XContentFactory.jsonBuilder()
        .startObject()
          .field("title", "Relationships are cool")
          .field("body", "It's not complicated at all...")
          .startObject("user")
            .field("id", 3)
            .field("name", "Alice John")
          .endObject()
        .endObject()));
    
    BulkResponse bulkResponse = bulkRequest.execute().actionGet();
    if (bulkResponse.hasFailures()) {
        // process failures by iterating through each bulk response item
      System.err.println("Bulk requesting has failed.");
    } else {
      System.out.println("Bulk requesting has succeeded.");
    }
  }

  @Test
  public void testFieldCollapsing() {
    SearchRequestBuilder srb = client.prepareSearch(indexName).setTypes(typeBlogpost)
      .setQuery(
        QueryBuilders.boolQuery()
          .must(QueryBuilders.matchQuery("title", "relationships"))
          .must(QueryBuilders.matchQuery("user.name", "john")))
      .addAggregation(AggregationBuilders
          .terms("users")
          .field("user.name.raw")
          .order(Order.aggregation("top_score", false))
          .subAggregation(AggregationBuilders.topHits("blogposts").setSize(5).setFetchSource(true))
          .subAggregation(AggregationBuilders.max("top_score").script("_score").lang("groovy")));
    
    // execute the srb
    SearchResponse response = srb.execute().actionGet();
    
    // read agg
    // TODO: why the top_score is 0
    System.out.println(response);
  }

}
