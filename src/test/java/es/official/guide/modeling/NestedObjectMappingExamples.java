package es.official.guide.modeling;

import java.io.IOException;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;

import es.ESTestBase;

/**
 * http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/nested-mapping.html 
 * http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/nested-query.html
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
    
  }

}
