package de.artcom_venture.elasticsearch.followup;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Random;
import java.util.Scanner;

import junit.framework.TestCase;

import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.plugins.PluginInfo;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.json.JSONArray;
import org.json.JSONObject;

public class FollowUpPluginTest extends TestCase {

    private Client client;
    private static Random randomGenerator = new Random();
    private static final String ES_INDEX = "myindex";
    private static final String ES_TYPE = "mytype";
    private static final String PLUGIN_NAME = "followup";
    private static final String HOST = "127.0.0.1";
    private static final String PLUGIN_URL_LIST = "http://" + HOST + ":9200/" + ES_INDEX + "/_" + PLUGIN_NAME;

    @Override
    public void setUp() throws Exception {
    	client = TransportClient.builder().build().addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(HOST), 9300));
	    
	    // create index
    	client.admin().indices().delete(new DeleteIndexRequest("_all")).actionGet();
    	client.admin().indices().create(Requests.createIndexRequest(ES_INDEX)).actionGet();
	    
	    // create type
	    XContentBuilder mapping = XContentFactory.jsonBuilder()
	        .startObject()
	             .startObject(ES_TYPE)
	                  .startObject("properties")
	                      .startObject("key")
	                          .field("type", "string")
	                          .field("index", "not_analyzed")
	                       .endObject()
	                       .startObject("value")
	                          .field("type","long")
	                       .endObject()
	                  .endObject()
	              .endObject()
	           .endObject();
	    client.admin().indices().preparePutMapping(ES_INDEX).setType(ES_TYPE).setSource(mapping).execute().actionGet();
	    
	    // create initial document
	    clearListener();
	    indexDocument("1");
    }

    @Override
    public void tearDown() throws Exception {
    	client.close();
    }
    
    private int getChangesLength() throws MalformedURLException, IOException {
    	Scanner scanner = new Scanner(new URL(PLUGIN_URL_LIST).openStream());
		JSONObject response =  new JSONObject(scanner.useDelimiter("\\Z").next());
		scanner.close();
		return response.getJSONArray(ES_INDEX).length();
    }
    
    private int stopListener() throws MalformedURLException, IOException {
    	Scanner scanner = new Scanner(new URL(PLUGIN_URL_LIST + "?stop").openStream());
		JSONObject response =  new JSONObject(scanner.useDelimiter("\\Z").next());
		scanner.close();
		return response.getInt("status");
    }
    
    private int startListener() throws MalformedURLException, IOException {
    	Scanner scanner = new Scanner(new URL(PLUGIN_URL_LIST + "?start").openStream());
		JSONObject response =  new JSONObject(scanner.useDelimiter("\\Z").next());
		scanner.close();
		return response.getInt("status");
    }

    private int clearListener() throws MalformedURLException, IOException {
    	Scanner scanner = new Scanner(new URL(PLUGIN_URL_LIST + "?clear").openStream());
		JSONObject response =  new JSONObject(scanner.useDelimiter("\\Z").next());
		scanner.close();
		return response.getInt("status");
    }
    
    private void indexDocument(String id) throws IOException {
    	XContentBuilder sourceBuilder = XContentFactory.jsonBuilder()
    		.startObject()
    			.field("key", "Lorem ipsum")
    			.field("value", randomGenerator.nextLong())
    		.endObject();
    	client.index(new IndexRequest(ES_INDEX, ES_TYPE).source(sourceBuilder).id(id)).actionGet();
    }
    
    private void deleteDocument(String id) throws IOException {
    	client.prepareDelete(ES_INDEX, ES_TYPE, id).execute().actionGet();
    }
    
    private void createDocument() throws IOException {
    	XContentBuilder sourceBuilder = XContentFactory.jsonBuilder()
    		.startObject()
    			.field("key", "Lorem ipsum")
    			.field("value", randomGenerator.nextLong())
    		.endObject();
    	client.index(new IndexRequest(ES_INDEX, ES_TYPE).source(sourceBuilder)).actionGet();
    }
    
    public void testCompatibility() throws Exception {
    	String pluginVersion = "-";
    	NodesInfoResponse nodesInfoResponse = client.admin().cluster().prepareNodesInfo().clear().setPlugins(true).get();
        for (PluginInfo pluginInfo : nodesInfoResponse.getNodes()[0].getPlugins().getPluginInfos()) {
        	if (pluginInfo.getName().equals(PLUGIN_NAME)) {
        		pluginVersion = pluginInfo.getVersion();
        	}
        }
        assertTrue(pluginVersion.startsWith(client.admin().cluster().prepareNodesInfo().get().getNodes()[0].getVersion().toString()));
    }
    
    public void testStart() throws MalformedURLException, IOException {
    	assertEquals(200, startListener());
    }

    public void testStop() throws MalformedURLException, IOException {
    	assertEquals(200, stopListener());
    }

    public void testClear() throws MalformedURLException, IOException {
    	assertEquals(200, clearListener());
    }
    
    public void testListener() throws Exception {
		assertEquals(0, getChangesLength());
		
		// not listening
		indexDocument("2");
		assertEquals(0, getChangesLength());
		
		// listening
		assertEquals(200, startListener());
		indexDocument("3");
		assertEquals(1, getChangesLength());

		// start resets the list
		assertEquals(200, startListener());
		assertEquals(0, getChangesLength());
		indexDocument("4");
		indexDocument("5");
		assertEquals(2, getChangesLength());
		
		// not listening
		assertEquals(200, stopListener());
		indexDocument("6");
		assertEquals(2, getChangesLength());
    }
    
    public void testBuffer() throws Exception {
		startListener();
		assertEquals(0, getChangesLength());
		createDocument();
		indexDocument("a");
		deleteDocument("a");
		indexDocument("b");
		stopListener();
		assertEquals(4, getChangesLength());
		
		// latest changes stored
    	Scanner scanner = new Scanner(new URL(PLUGIN_URL_LIST).openStream());
		JSONObject response =  new JSONObject(scanner.useDelimiter("\\Z").next());
		scanner.close();
		JSONArray changes = response.getJSONArray(ES_INDEX);
		
		assertEquals("CREATE", changes.getJSONObject(0).getString("operation"));
		
		assertEquals("a", changes.getJSONObject(1).getString("id"));
		assertEquals("INDEX", changes.getJSONObject(1).getString("operation"));
		
		assertEquals("a", changes.getJSONObject(2).getString("id"));
		assertEquals("DELETE", changes.getJSONObject(2).getString("operation"));
		
		assertEquals("b", changes.getJSONObject(3).getString("id"));
		assertEquals("INDEX", changes.getJSONObject(3).getString("operation"));
    }
}
