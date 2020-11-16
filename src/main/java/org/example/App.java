package org.example;

import com.fasterxml.jackson.core.JsonParser;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.ml.job.results.Bucket;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException, ParseException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("127.0.0.1", 9200, "http"),
                        new HttpHost("127.0.0.1", 9201, "http"),
                        new HttpHost("127.0.0.1", 9202, "http")));

        //Création des trois comptes
        System.out.println(" >>>> Clients insertion");
        IndexResponse indexResponse = insertAccount1(client);
        System.out.println("Client 1: "+indexResponse.status());
        indexResponse = insertAccount2(client);
        System.out.println("Client 2: "+indexResponse.status());
        indexResponse = insertAccount3(client);
        System.out.println("Client 3: "+indexResponse.status());

        //Affichage des comptes;
        System.out.println(" >>>> Clients:");
        System.out.println(getResponse(client));

        //Insertion des 1000 comptes grâce au bulk
        System.out.println(" >>>> Bulk Creation");
        System.out.println(insertInBulk(client).status());

        //Search 100 premiers comptes
        question31(client);

        //Recherche 150 à 200
        quesion32(client);

        //Search balance > 10000 et state != 'TX'
        question33(client);

        // Search state = 'TX', age > 29 and  20000 < balance < 30000
        question34a(client);

        // Search state = 'TX', age > 29 and  20000 < balance < 30000 other methode
        question34b(client);

        //Middle age, number of employees by state, sum of balance, average of balance
        question35(client);

        client.close();
    }

    /*
    Question 3.1
     */
    private static void question31(RestHighLevelClient monClient) throws IOException {
        SearchRequest searchRequest = new SearchRequest("account");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        sourceBuilder.from(0); //departure
        sourceBuilder.size(100); // number of results display
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = monClient.search(searchRequest, RequestOptions.DEFAULT);

        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits(); // get all documents
        System.out.println("Results of the question 3.1= \n size results:"+searchHits.length);
        for(SearchHit hit : searchHits){
            System.out.println(hit.toString()); //display each document
        }

    }

    /*
    Question 3.2
     */
    private static void quesion32(RestHighLevelClient monClient) throws IOException {
        SearchRequest searchRequest = new SearchRequest("account");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        sourceBuilder.from(150); // departure of result
        sourceBuilder.size(50); // number of result display
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = monClient.search(searchRequest, RequestOptions.DEFAULT);

        SearchHits hits = searchResponse.getHits(); // get all documents
        SearchHit[] searchHits = hits.getHits();
        System.out.println("Results of the question 3.2= \n size results:"+searchHits.length);
        for(SearchHit hit : searchHits){
            System.out.println(hit.toString()); // display each document
        }
    }

    /*
    Question 3.3
     */
    private static void question33(RestHighLevelClient monClient) throws IOException {
        SearchRequest request = new SearchRequest("account");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
                .mustNot(QueryBuilders.matchQuery("state", "TX"))
                .filter(QueryBuilders.rangeQuery("balance").gte(10000)); // create search filter
        searchSourceBuilder.query(boolQuery); // add filter in the search request
        request.source(searchSourceBuilder); // add search filter in the global request

        SearchResponse searchResponse = monClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits(); // get all documents
        SearchHit[] searchHits = hits.getHits();
        System.out.println("Results of the question 3.3=  " +"\n"+
                "size results:"+searchHits.length);
        for(SearchHit hit : searchHits){
            System.out.println(hit.toString()); // display each document
        }

    }

    /*
    Question 3.4a
     */
    private static void question34a(RestHighLevelClient monClient) throws IOException {
        SearchRequest request = new SearchRequest("account");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("state", "TX"))
                .filter(QueryBuilders.rangeQuery("balance").lte(30000).gte(20000))
                .filter(QueryBuilders.rangeQuery("age").gte(29)); // create search filter
        searchSourceBuilder.query(boolQuery); // add filter in the search request
        request.source(searchSourceBuilder); // add search filter in the global request

        SearchResponse searchResponse = monClient.search(request, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits(); // get all documents
        SearchHit[] searchHits = hits.getHits();
        System.out.println("Results of the question 3.4a= \n size results:"+searchHits.length);
        for(SearchHit hit : searchHits){
            System.out.println(hit.toString()); // display each document
        }
    }

    /*
       Question 3.4a
    */
    private static void question34b(RestHighLevelClient monClient) throws IOException {
        SearchRequest request = new SearchRequest("account");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        BoolQueryBuilder bool = QueryBuilders.boolQuery()
                        .filter(QueryBuilders.queryStringQuery("TX").defaultField("state"))
                        .filter(QueryBuilders.rangeQuery("balance").lte(30000).gte(20000))
                        .filter(QueryBuilders.rangeQuery("age").gte(29)); // create search filter

        searchSourceBuilder.query(bool); // add filter in the search request
        request.source(searchSourceBuilder); // add search filter in the global request

        SearchResponse searchResponse = monClient.search(request, RequestOptions.DEFAULT); //execution of the request
        SearchHits hits = searchResponse.getHits();  // get all documents
        SearchHit[] searchHits = hits.getHits();
        System.out.println("Results of the question 3.4b= \n size results:"+searchHits.length);
        for(SearchHit hit : searchHits){
            System.out.println(hit.toString()); //display each result in hits
        }
    }

    /*
    Question 3.5
     */
    private static void question35(RestHighLevelClient monClient) throws IOException {
        SearchRequest request = new SearchRequest("account");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery()); // get all result

        AvgAggregationBuilder avg = AggregationBuilders.avg("average_age").field("age"); // creation of the aggregation average age
        searchSourceBuilder.aggregation(avg);// add aggregation in the search request
        AvgAggregationBuilder avgc = AggregationBuilders.avg("average_balance").field("balance"); // creation of the aggregation average balance
        searchSourceBuilder.aggregation(avgc);// add aggregation in the search request
        SumAggregationBuilder sum = AggregationBuilders.sum("sum_balance").field("balance"); // creation of the aggregation sum balance
        searchSourceBuilder.aggregation(sum); // add aggregation in the search request
        TermsAggregationBuilder aggregation =
                AggregationBuilders.terms("Group_state").field("state.keyword").size(1000)
                        .subAggregation(AggregationBuilders.cardinality("employes").field("account_number")); // creation of the aggregation group by state and get number of employees in each state
        searchSourceBuilder.aggregation(aggregation); // add the aggregration in the search request
        request.source(searchSourceBuilder); // add the search request in the request

        SearchResponse searchResponse = monClient.search(request, RequestOptions.DEFAULT); // execution of the request

        Aggregations aggregations = searchResponse.getAggregations(); //get all aggregation in the search response
        Avg averageAge = aggregations.get("average_age"); //get aggregation average age
        Avg averageBalance = aggregations.get("average_balance"); // get aggregation average balance
        Sum somme = aggregations.get("sum_balance"); // get aggregation sum_balance

        Terms agg = searchResponse.getAggregations().get("Group_state"); // get aggregation Group_state
        Collection<Terms.Bucket> buckets = (Collection<Terms.Bucket>) agg.getBuckets(); // get all results in the aggregation Group_state

        System.out.println("Results of the question 3.5 = ");
        for (Terms.Bucket bucket : buckets) {
            Cardinality terms = bucket.getAggregations().get("employes"); // get subaggregation employees
            System.out.println("State: " + bucket.getKeyAsString() + ", number of employes: " + terms.getValue()); // Display state and number employees for each state
        }

        System.out.println("Average of balances: "+averageBalance.getValue()); // Display average of balance
        System.out.println("Sum of balances: "+somme.getValue()); // Display sum of balance
        System.out.println("Middle age: "+averageAge.getValue()); // Display middle age
    }

    /*
    Insert all client with help of bulk
     */
    private static BulkResponse insertInBulk(RestHighLevelClient monClient) throws ParseException, IOException {
        JSONParser parser = new JSONParser();
        ArrayList<JSONObject> json = getJson();
        BulkRequest request = new BulkRequest(); //creation bulk
        for(int i = 0; i < json.size(); i+=2){
            JSONObject index = (JSONObject) json.get(i).get("index");
            int id = Integer.parseInt(index.get("_id").toString()); //Transformation index to integer to create index for the bulk
            JSONObject account = json.get(i+1);
            request.add(new IndexRequest("account").id(String.valueOf(id)).source(account, XContentType.JSON)); // add the new client in the request
        }

        BulkResponse bulkResponse = monClient.bulk(request, RequestOptions.DEFAULT); // play bulk

        return  bulkResponse;
    }

    /*
    Get clients accounts in json file
     */
    private static ArrayList<JSONObject> getJson(){
        JSONParser parser = new JSONParser();
        ArrayList<JSONObject> objets = new ArrayList<JSONObject>();
        String line;
        try {
            BufferedReader input = new BufferedReader(
                    new InputStreamReader(new FileInputStream("accounts.json"))
            );
            try {
                while ((line=input.readLine())!=null) {
                    JSONObject obj = (JSONObject) parser.parse(line);
                    objets.add(obj);
                }
            } catch (IOException e) {
                System.err.println("Exception: interrupted I/O.");
            } catch (ParseException e) {
                e.printStackTrace();
            } finally {
                try {
                    input.close();
                } catch (IOException e) {
                    System.err.println("I/O exception: unable to close accounts.json");
                }
            }
        } catch (FileNotFoundException e) {
            System.err.println("Input file not found.");
        }

        return objets;
    }

    /*
    Get response after client insertion
     */
    private static String getResponse(RestHighLevelClient monClient) throws IOException {
        String response = "";
        GetRequest getRequest = new GetRequest("account", "1");
        GetResponse getResponse = monClient.get(getRequest, RequestOptions.DEFAULT); //Get client 1
        response += "Account 1: "+getResponse.toString()+"\n"; //Transform client 1 to string for display
        getRequest = new GetRequest("account", "2"); //Get client 2
        getResponse = monClient.get(getRequest, RequestOptions.DEFAULT);
        response += "Account 2: "+getResponse.toString()+"\n"; //Transform client 3 to string for display
        getRequest = new GetRequest("account", "3"); //Get client 2
        getResponse = monClient.get(getRequest, RequestOptions.DEFAULT);
        response += "Account 3: "+getResponse.toString()+"\n"; //Transform client 3 to string for display

        return response;
    }

    /*
    Insert client 1
     */
    private static IndexResponse insertAccount1(RestHighLevelClient monClient) throws IOException {
        IndexRequest request = new IndexRequest("account"); //index
        request.id("1"); //document id
        String jsonString = "{" +
                "\"account_number\": 1,"+
                "\"balance\":2," +
                "\"firstname\": \"MMMMM\","+
                "\"lastname\": \"BBBB\","+
                "\"age\": 24," +
                "\"gender\": \"F\"," +
                "\"address\": \"8 rue du petit four\"," +
                "\"employer\": \"Orange\"," +
                "\"email\": \"mmmmm.bbbb@gmail.com\","+
                "\"state\": \"France\","+
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        request.source(jsonString, XContentType.JSON); //Add json body to the put request
        IndexResponse indexResponse = monClient.index(request, RequestOptions.DEFAULT); //response of request (OK, 400...)

        return indexResponse;
    }

    /*
    insert client 2
     */
    private static IndexResponse insertAccount2(RestHighLevelClient monClient) throws IOException {
        IndexRequest request = new IndexRequest("account"); //index
        request.id("2"); //document id
        String jsonString = "{" +
                "\"account_number\": 2," +
                "\"balance\":15," +
                "\"firstname\": \"AAAAA\"," +
                "\"lastname\": \"CCCC\"," +
                "\"age\": 50," +
                "\"gender\": \"M\"," +
                "\"address\": \"8 rue du petit moulin\"," +
                "\"employer\": \"Fnac\"," +
                "\"email\": \"aaaaa.cccc@gmail.com\"," +
                "\"state\": \"USA\"" +
                "}";
        request.source(jsonString, XContentType.JSON); //Add json body to the put request
        IndexResponse indexResponse = monClient.index(request, RequestOptions.DEFAULT); //response of request (OK, 400...)

        return indexResponse;
    }

    /*
    insert client 3
     */
    private static IndexResponse insertAccount3(RestHighLevelClient monClient) throws IOException {
        IndexRequest request = new IndexRequest("account"); //index
        request.id("3"); // document id
        String jsonString = "{" +
                "\"account_number\": 3," +
                "\"balance\":98," +
                "\"firstname\": \"GGGGG\"," +
                "\"lastname\": \"DDDDD\"," +
                "\"age\": 78," +
                "\"gender\": \"M\"," +
                "\"address\": \"20 rue du lavoir\"," +
                "\"employer\": \"SG\"," +
                "\"email\": \"ggggg.ddddd@gmail.com\"," +
                "\"state\": \"Espagne\"" +
                "}"; // Creation of json body
        request.source(jsonString, XContentType.JSON); //Add json body to the put request

        IndexResponse indexResponse = monClient.index(request, RequestOptions.DEFAULT);//Get the response status (OK, or Status 404...)

        return indexResponse;
    }
}
