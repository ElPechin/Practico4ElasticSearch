import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import spark.Spark;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.http.client.methods.RequestBuilder.post;
import static spark.Spark.port;

public class App {

    //The config parameters for the connection
    private static final String HOST = "localhost";
    private static final int PORT_ONE = 9200;
    private static final int PORT_TWO = 9201;
    private static final String SCHEME = "http";

    private static RestHighLevelClient restHighLevelClient;
    private static ObjectMapper objectMapper = new ObjectMapper();

    private static final String INDEX = "itemdata";
    private static final String TYPE = "item";

    /**
     * Implemented Singleton pattern here
     * so that there is just one connection at a time.
     *
     * @return RestHighLevelClient
     */
    private static synchronized RestHighLevelClient makeConnection() {

        if (restHighLevelClient == null) {
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost(HOST, PORT_ONE, SCHEME),
                            new HttpHost(HOST, PORT_TWO, SCHEME)));
        }

        return restHighLevelClient;
    }

    private static synchronized void closeConnection() throws IOException {
        restHighLevelClient.close();
        restHighLevelClient = null;
    }

    private static Item insertItem (Item item) {

        Map<String, Object> dataMap = new HashMap<String, Object>();

        dataMap.put("id", item.getId());
        dataMap.put("siteId", item.getSiteId());
        dataMap.put("title", item.getTitle());
        dataMap.put("subtitle", item.getSubtitle());
        dataMap.put("sellerId", item.getSellerId());
        dataMap.put("categoryId", item.getCategoryId());
        dataMap.put("price", item.getPrice());
        dataMap.put("currencyId", item.getCurrencyId());
        dataMap.put("availableQuantity", item.getAvailableQuantity());
        dataMap.put("condition", item.getCondition());
        dataMap.put("acceptsMercadopago", item.getAcceptsMercadopago());
        dataMap.put("status", item.getStatus());

        IndexRequest indexRequest = new IndexRequest(INDEX, TYPE, item.getId())
                .source(dataMap);
        try {
            System.out.println("asdasdsa");
            IndexResponse response = makeConnection().index(indexRequest, RequestOptions.DEFAULT);
            System.out.println("asdaasddsa");
        } catch (ElasticsearchException e) {
            System.out.println(e.getDetailedMessage());
            e.getDetailedMessage();
        } catch (IOException ex) {
            System.out.println(ex.getLocalizedMessage());
            ex.getLocalizedMessage();
        }
        return item;
    }

    private static Item getItemById(String id) {
        GetRequest getPersonRequest = new GetRequest(INDEX, TYPE, id);
        GetResponse getResponse = null;
        try {
            getResponse = restHighLevelClient.get(getPersonRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.getLocalizedMessage();
        }
        System.out.printf("as");
        return getResponse != null ?
                objectMapper.convertValue(getResponse.getSourceAsMap(), Item.class) : null;
    }

    private static Item updateItemById(String id, Item item) {
        UpdateRequest updateRequest = new UpdateRequest(INDEX, TYPE, id)
                .fetchSource(true);    // Fetch Object after its update
        try {
            String itemJson = objectMapper.writeValueAsString(item);
            updateRequest.doc(itemJson, XContentType.JSON);
            UpdateResponse updateResponse = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
            return objectMapper.convertValue(updateResponse.getGetResult().sourceAsMap(), Item.class);
        } catch (JsonProcessingException e) {
            e.getMessage();
        } catch (IOException e) {
            e.getLocalizedMessage();
        }
        System.out.println("Unable to update person");
        return null;
    }

    private static void deleteItemById(String id) {
        DeleteRequest deleteRequest = new DeleteRequest(INDEX, TYPE, id);
        try {
            DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.getLocalizedMessage();
        }
    }

    public static void main(String[] args) throws IOException {

        makeConnection();

        port(9000);

        Spark.post("/items", ((request, response) -> {
            response.type("application/json");
            Item item = new Gson().fromJson(request.body(), Item.class);

            item = insertItem(item);
            System.out.println("Item inserted --> " + item.getId());
            return new Gson().toJson(new StandardResponse(StatusResponse.SUCCES));
        }));

        Spark.get("/items/:id", ((request, response) -> {
            response.type("application/json");
            return new Gson().toJson(new StandardResponse(StatusResponse.SUCCES, new Gson().toJson(getItemById(request.params("id")))));
        }));

       /* Spark.delete("/itemDelete/:id", ((request, response) -> {

        }));*/




        /*item.setId("MLA695166125");
        item.setSiteId("MLA");
        item.setSubtitle("null");
        item.setSellerId(128885);
        item.setCategoryId("MLA1055");
        item.setPrice(55998);
        item.setCurrencyId("ARS");
        item.setAvailableQuantity(1);
        item.setCondition("New");
        item.setPictures(null);
        item.setAcceptsMercadopago(true);
        item.setStatus("active");
        item.setDateCreated("2017-12-02T17:09:37.000Z");
        item.setLastUpdated("2019-05-09T22:13:38.000Z");*/
        //item.setName("Shubham");
       /* item = insertItem(item);
        System.out.println("Item inserted --> " + item.getId());

        item = getItemById(item.getId());
        System.out.printf("Item searched --> " + item.getId());*/

/*
        System.out.println("Changing name to `Shubham Aggarwal`...");
        person.setName("Shubham Aggarwal");
        updatePersonById(person.getPersonId(), person);
        System.out.println("Person updated  --> " + person);

        System.out.println("Getting Shubham...");
        Person personFromDB = getPersonById(person.getPersonId());
        System.out.println("Person from DB  --> " + personFromDB);

        System.out.println("Deleting Shubham...");
        deletePersonById(personFromDB.getPersonId());
        System.out.println("Person Deleted");
*/
        //closeConnection();
    }




}