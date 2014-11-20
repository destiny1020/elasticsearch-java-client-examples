package es;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

public class BasicExamples {

    public static void main(String[] args) throws IOException {
        Node node = null;
        Client client = null;
        try {

            // Instantiates an ElasticSearch cluster node in the
            // current VM. The behavior of this node is defined by the
            // settings we specify. "client(true)" indicates that we
            // going to be a pure client node, which means it will
            // hold no index data and other optimizations are applied
            // by different modules.
            // The node tries to find a master node to connect too and
            // has a default timeout of 30 seconds.
            node = NodeBuilder.nodeBuilder().client(true).node();

            // This is actually the object that allows us to execute
            // commands against our local node and by this the entire
            // cluster.
            client = node.client();

            // Test 1: check the cluster status
            ClusterHealthResponse hr = null;
            try {
                hr = client.admin().cluster().prepareHealth()
                        .setWaitForGreenStatus()
                        .setTimeout(TimeValue.timeValueMillis(250)).execute()
                        .actionGet();
            } catch (MasterNotDiscoveredException e) {
                // No cluster status since we don't have a cluster
            }

            if (hr != null) {
                System.out.println("Data nodes found:"
                        + hr.getNumberOfDataNodes());
                System.out.println("Timeout? :" + hr.isTimedOut());
                System.out.println("Status:" + hr.getStatus().name());
            }
            
            String indexName = "java_client";
            String typeName = "java_type";
            
            // Test 2: delete the index if existed
            DeleteIndexRequestBuilder dirb = client.admin().indices().prepareDelete(indexName);
            DeleteIndexResponse deleteIndexResponse = null;
            
            try {
                deleteIndexResponse = dirb.execute().actionGet();
            } catch (IndexMissingException e) {
                // Index not found
                e.printStackTrace();
            }
            
            if(deleteIndexResponse != null && deleteIndexResponse.isAcknowledged()) {
                // Index deleted
                System.out.println("Index deleted !");
            } else {
                // Index deletion failed
                System.err.println("Index deletion deleted !");
            }

            // Test 3: create an index

            CreateIndexRequestBuilder cirb = client.admin().indices()
                    .prepareCreate(indexName);

            HashMap<String, Object> settings = new HashMap<>();

            //Choosing the number of shards is actually an important one.
            //This is well documented in the official documentation.
            //The default value is 5, but we keep all data in 1 single shard
            settings.put("number_of_shards", 1);

            //Number of replication of indices within the cluster
            settings.put("number_of_replicas", 1);

            cirb.setSettings(settings);

            CreateIndexResponse createIndexResponse = null;
            try {
                createIndexResponse = cirb.execute().actionGet();
            } catch (IndexAlreadyExistsException e) {
                // Index already exists
                e.printStackTrace();
            }

            if (createIndexResponse != null
                    && createIndexResponse.isAcknowledged()) {
                // Index created
                System.out.println("Index created !");
            } else {
                // Index creation failed
                System.err.println("Index creation failed !");
            }

            // Test 4: create type/mapping
            XContentBuilder builder = XContentFactory.jsonBuilder().
                    startObject().
                       startObject(typeName).
                          field("dynamic", "strict").
                          startObject("_id").
                               field("path", "id").
                          endObject().
                          startObject("_all").
                               field("enabled", "true").
                          endObject().
                          startObject("properties").
                               startObject("id").
                                   field("type", "long").
                                   field("store", "yes").
                                   field("index", "not_analyzed").
                               endObject().
                               startObject("country_code").
                                   field("type", "string").
                                   field("store", "yes").
                                   field("index", "not_analyzed").
                               endObject().
                               startObject("names").
                                   field("type", "string"). // (*)
                                   field("store", "yes").
                                   field("index", "analyzed").
                               endObject().
                               startObject("postal_codes").
                                   field("type", "string").
                                   field("store", "yes"). 
                                   field("index", "analyzed").
                               endObject().
                          endObject().
                      endObject().
                  endObject();

            PutMappingResponse response = client.admin().indices()
                    .preparePutMapping(indexName).setType(typeName)
                    .setSource(builder).execute().actionGet();

            if (response.isAcknowledged()) {
                // Type and Mapping created!
                System.out.println("Type and mapping created !");
            } else {
                // Failed to create type and mapping
                System.err.println("Type and mapping creation failed !");
            }
            
            // Test 5: create type/mappings with nested object
            String typeNameNested = "java_type_nested";
            builder = XContentFactory.jsonBuilder().
                    startObject().
                       startObject(typeNameNested).
                          field("dynamic", "strict").
                          startObject("_id").
                             field("path", "id").
                          endObject().
                          startObject("_all").
                             field("enabled", "true").
                          endObject().
                          startObject("properties").
                              startObject("id").
                                 field("type", "long").
                                 field("store", "yes").
                                 field("index", "not_analyzed").
                              endObject().
                              startObject("country_code").
                                 field("type", "string").
                                 field("store", "yes").
                                 field("index", "not_analyzed").
                              endObject().
                              startObject("postal_codes").
                                 field("type", "nested").
                                 startObject("properties").
                                     startObject("code").
                                         field("type", "string").
                                         field("store", "yes").
                                         field("index", "analyzed").
                                     endObject().
                                     startObject("names").
                                         field("type", "nested").
                                         startObject("properties").
                                             startObject("name").
                                                 field("type", "string").
                                                 field("store", "yes").
                                                 field("index", "analyzed").
                                             endObject().
                                         endObject().
                                     endObject().
                                 endObject().
                              endObject().
                         endObject().
                    endObject();
            
            PutMappingResponse nestedResponse = client.admin().indices()
                    .preparePutMapping(indexName).setType(typeNameNested)
                    .setSource(builder).execute().actionGet();

            if (nestedResponse.isAcknowledged()) {
                // Nested Type and Mapping created!
                System.out.println("Type and mapping with nested created !");
            } else {
                // Failed to create type and mapping with nested
                System.err.println("Type and mapping with nested creation failed !");
            }
            
            // Test 6: create mapping with json text
//            String mappingString="{"\"dynamic\":\"strict\",\"_id\":{\"path\":\"id\"},\"properties\":{\"country_code\":{\"type\":\"string\",\"index\":\"not_analyzed\",\"store\":true},\"id\":{\"type\":\"long\",\"store\":true},\"names\":{\"type\":\"string\",\"store\":true},\"postal_codes\":{\"type\":\"string\",\"store\":true}}}";
//            
//            PutMappingResponse response=client.admin().
//                                          indices().
//                                          preparePutMapping("myindexname").
//                                          setType("mytypename").
//                                          setSource(mappingString).
//                                          execute().
//                                          actionGet();
            
            // Test 7: read mapping of an index and type
            IndexMetaData imd = null;
            try {
                ClusterState cs = client.admin().
                                     cluster().
                                     prepareState().
                                     setIndices(indexName).
                                     execute().
                                     actionGet().
                                     getState();
                
                imd = cs.getMetaData().index(indexName);
            }
            catch (IndexMissingException e) {
                // If there is no index, there is no mapping either
                e.printStackTrace();
            }
             
            MappingMetaData mdd = imd.mapping(typeName);
             
            if (mdd == null) {
                // No mapping found
                System.err.println(String.format("No mapping found: Index - %s, Type - %s", indexName, typeName));
            }
            else {
                System.out.println("Mapping as JSON string:" + mdd.source());
            }
            
            // Test 8: check whether an index/type exists
            // index/type check
            boolean typeExists = client.admin().indices().
                prepareTypesExists(indexName).
                setTypes(typeName).
                execute().
                actionGet().
                isExists();
            
            System.out.println(String.format("Index: %s, Type: %s, exists: %s", indexName, typeName, typeExists));

            // index check
            boolean indexExists = client.admin().indices().
                    prepareExists(indexName).
                    execute().
                    actionGet().
                    isExists();
            
            System.out.println(String.format("Index: %s, exists: %s", indexName, indexExists));
            
            // Test 9: add an alias for index
            String indexAlias = "java_alias";
            try {
                client.admin().
                       indices().
                       prepareAliases().
                       addAlias(indexName, indexAlias).
                       execute().
                       actionGet();
            }
            catch(IndexMissingException e) {
                // Index not found
                e.printStackTrace();
            }
            
            // Test 10: switch alias for index, switching is a single atomic operation
            String indexNewAlias = "java_new_alias";
            client.admin().indices().
                prepareAliases().
                addAlias(indexName, indexNewAlias).
                removeAlias(indexName, indexAlias).
                execute().
                actionGet();
            
            // Test 11: get alias for index
            ImmutableOpenMap<String, AliasMetaData> iom =
                    client.admin().
                           cluster().
                           state(new ClusterStateRequest()).
                           actionGet().
                           getState().
                           getMetaData().
                           aliases().
                           get(indexNewAlias);
      
            if (iom==null) {
                // alias not found.
                System.err.println("Alias not found.");
            }
  
            Iterator<ObjectObjectCursor<String, AliasMetaData>> i = iom.iterator();
  
            while(i.hasNext()) {
                ObjectObjectCursor<String, AliasMetaData> ooc=i.next();
                System.out.println("Index = "+ ooc.key +"/Alias = "+
                           ooc.value.getAlias());
            }
            
        } finally {
            if (node != null)
                node.close();
        }
    }
}
