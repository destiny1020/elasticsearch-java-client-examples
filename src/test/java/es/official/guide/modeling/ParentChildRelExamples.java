package es.official.guide.modeling;

import java.io.IOException;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.junit.Test;

import es.ESTestBase;

/**
 * http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/parent-child-mapping.html
 * http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/has-child.html
 * http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/has-parent.html
 * http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/children-agg.html
 */
public class ParentChildRelExamples extends ESTestBase {

  private String indexName = "company";
  private String typeBranch = "branch";
  private String typeEmployee = "employee";

  @Test
  public void testCreateCompanyIndex() throws ElasticsearchException, IOException {
    CreateIndexResponse response =
        client
            .admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(
                XContentFactory.jsonBuilder().field("number_of_shards", 1)
                    .field("number_of_replicas", 0)).execute().actionGet();

    if (response.isAcknowledged()) {
      System.out.println("Index creation succeeded.");
    } else {
      System.err.println("Index creation failed.");
    }
  }

  @Test
  public void testPuttingMappings() throws IOException {
    // cannot support creating multiple mappings for types in a single call ?
    // ATTENSION: BOTH setType(typeName) and the startObject(typeName) are needed, but startObject("mappings") is not needed
    PutMappingRequestBuilder pmrb1 = client.admin().indices().preparePutMapping(indexName).setType(typeBranch).setSource(XContentFactory.jsonBuilder()
        .startObject()
//          .startObject("mappings")
          .startObject(typeBranch)
          .endObject()
//          .endObject()
        .endObject());
    
    PutMappingRequestBuilder pmrb2 = client.admin().indices().preparePutMapping(indexName).setType(typeEmployee).setSource(XContentFactory.jsonBuilder()
        .startObject()
//          .startObject("mappings")
          .startObject(typeEmployee)
            .startObject("_parent")
              .field("type", "branch")
            .endObject()
          .endObject()
//          .endObject()
        .endObject());
    
    System.out.println(pmrb1.request().source());
    System.out.println(pmrb2.request().source());
    
    PutMappingResponse response1 = pmrb1.execute().actionGet();
    PutMappingResponse response2 = pmrb2.execute().actionGet();
    System.out.println(response1);
    System.out.println(response2);
  }
  
  @Test
  public void testIndexingParentDoc() throws IOException {
    BulkRequestBuilder brb = client.prepareBulk();
    
    brb.add(client.prepareIndex(indexName, typeBranch, "london").setSource(XContentFactory.jsonBuilder()
        .startObject()
          .field("name", "London Westminster")
          .field("city", "London")
          .field("country", "UK")
        .endObject()));
    
    brb.add(client.prepareIndex(indexName, typeBranch, "liverpool").setSource(XContentFactory.jsonBuilder()
        .startObject()
          .field("name", "Liverpool Central")
          .field("city", "Liverpool")
          .field("country", "UK")
        .endObject()));
    
    brb.add(client.prepareIndex(indexName, typeBranch, "paris").setSource(XContentFactory.jsonBuilder()
        .startObject()
          .field("name", "Champs Élysées")
          .field("city", "Paris")
          .field("country", "France")
        .endObject()));
    
    BulkResponse response = brb.execute().actionGet();
    if(response.hasFailures()) {
      System.err.println(response.buildFailureMessage());
    } else {
      System.out.println("Bulk indexing succeeded.");
    }
  }
  
  /**
   * Routing value is the parent value
   */
  @Test
  public void testIndexingChildDoc() throws IOException {
    BulkRequestBuilder brb = client.prepareBulk();
    
    // children document should have a field named "_parent", not only setRouting is not enough !!!
    brb.add(client.prepareIndex(indexName, typeEmployee, "1").setRouting("london").setSource(XContentFactory.jsonBuilder()
        .startObject()
          .field("_parent", "london")
          .field("name", "Alice Smith")
          .field("dob", "1970-10-24")
          .field("hobby", "hiking")
        .endObject()));
    
    brb.add(client.prepareIndex(indexName, typeEmployee, "2").setRouting("london").setSource(XContentFactory.jsonBuilder()
        .startObject()
          .field("_parent", "london")
          .field("name", "Mark Thomas")
          .field("dob", "1982-05-16")
          .field("hobby", "diving")
        .endObject()));
    
    brb.add(client.prepareIndex(indexName, typeEmployee, "3").setRouting("liverpool").setSource(XContentFactory.jsonBuilder()
        .startObject()
          .field("_parent", "liverpool")
          .field("name", "Barry Smith")
          .field("dob", "1979-04-01")
          .field("hobby", "hiking")
        .endObject()));
    
    brb.add(client.prepareIndex(indexName, typeEmployee, "4").setRouting("paris").setSource(XContentFactory.jsonBuilder()
        .startObject()
          .field("_parent", "paris")
          .field("name", "Adrien Grand")
          .field("dob", "1987-05-11")
          .field("hobby", "horses")
        .endObject()));
    
    BulkResponse response = brb.execute().actionGet();
    if(response.hasFailures()) {
      System.err.println(response.buildFailureMessage());
    } else {
      System.out.println("Bulk indexing succeeded.");
    }
  }
  
  @Test
  public void testFindParentsByChildren() {
    SearchResponse response = client.prepareSearch(indexName).setQuery(
        // 1987-10-20 can meet the criteria
        QueryBuilders.hasChildQuery(typeEmployee, QueryBuilders.rangeQuery("dob").gte("1980-01-01")))
        .execute().actionGet();
    
    // read response
    System.out.println(response);
  }
  
  @Test
  public void testFindParentsByChildrenMatch() {
    SearchResponse response = client.prepareSearch(indexName).setQuery(
        QueryBuilders.hasChildQuery(typeEmployee, QueryBuilders.matchQuery("name", "Alice Smith")).scoreType("max"))
        .execute().actionGet();
    
    // read response
    System.out.println(response);
  }
  
  @Test
  public void testFindParentsWithMinChildren() {
    SearchResponse response = client.prepareSearch(indexName).setQuery(
        QueryBuilders.hasChildQuery(typeEmployee, QueryBuilders.matchAllQuery()).minChildren(2))
        .execute().actionGet();
    
    // read response
    System.out.println(response);
  }
  
  @Test
  public void testFindChildrenByParent() {
    SearchResponse response = client.prepareSearch(indexName).setQuery(QueryBuilders.hasParentQuery(typeBranch, QueryBuilders.matchQuery("country", "UK")))
      .execute().actionGet();
    
    // read response
    System.out.println(response);
  }
  
  /**
   * children agg as a direct analog to the nested agg
   */
  @Test
  public void testChildrenAgg() {
    // how we could determine the favourite hobbies of our employees by country
    // TODO: seems that ES 1.3.x does not support children aggregations
  }

}
