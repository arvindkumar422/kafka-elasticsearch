package com.kafka.consumer;

import com.google.gson.JsonParser;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class TwitterConsumerElastic {

  static Logger logger = LoggerFactory.getLogger(TwitterConsumerElastic.class.getName());

  public static RestHighLevelClient createClient() {

    String hostname = "kakfa-twitter-start-8087333619.us-east-1.bonsaisearch.net";
    String username = "xvgr157zoj"; // needed only for bonsai
    String password = "dm2zf85y2v"; // needed only for bonsai

    // credentials provider help supply username and password
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY,
            new UsernamePasswordCredentials(username, password));

    RestClientBuilder builder = RestClient.builder(
            new HttpHost(hostname, 443, "https"))
            .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
              @Override
              public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
              }
            });

    RestHighLevelClient client = new RestHighLevelClient(builder);
    return client;
  }

  public static KafkaConsumer<String, String> createConsumer(String topic) {

    String bootstrapServers = "127.0.0.1:9092";
    String groupId = "kafka-demo-elasticsearch";

    // create consumer configs
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit of offsets
    properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); // disable auto commit of offsets

    // create consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
    consumer.subscribe(Arrays.asList(topic));

    return consumer;

  }

  private static JsonParser jsonParser = new JsonParser();

  private static String extractIdFromTweet(String tweetJson) {
    return jsonParser.parse(tweetJson)
            .getAsJsonObject()
            .get("id_str")
            .getAsString();
  }

  public static void main(String[] args) throws IOException {

    RestHighLevelClient client = createClient();

    KafkaConsumer<String, String> consumer = createConsumer("tweets2");

    while (true) {
      ConsumerRecords<String, String> records =
              consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

      int recordCount = records.count();
      if(recordCount > 0) {logger.info("Received " + recordCount + " records");}
      BulkRequest bulkRequest = new BulkRequest();

      SearchRequest searchRequest = new SearchRequest("tweet-load");
      SearchSourceBuilder searchBuilder = new SearchSourceBuilder();
      searchBuilder.query(QueryBuilders.matchAllQuery());
      searchBuilder.size(0);
      searchRequest.source(searchBuilder);

      SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
      SearchHits hits = searchResponse.getHits();
      long totalHits = hits.getTotalHits().value;
      logger.info("hits  : " + totalHits);

      if(totalHits > 17) {
        DeleteIndexRequest delete_request = new DeleteIndexRequest("tweet-load");
        client.indices().delete(delete_request, RequestOptions.DEFAULT);
        CreateIndexRequest create_request = new CreateIndexRequest("tweet-load");
        client.indices().create(create_request, RequestOptions.DEFAULT);
      }
      for (ConsumerRecord<String, String> record : records) {

        try {
          String id = extractIdFromTweet(record.value());

          IndexRequest indexRequest = new IndexRequest("tweet-load")
                  .source(record.value(), XContentType.JSON)
                  .id(id); // this is to make our consumer idempotent

//          IndexResponse resp = client.index(indexRequest, RequestOptions.DEFAULT);
//          logger.info("Id is : " + resp.getId());

          bulkRequest.add(indexRequest); // we add to our bulk request (takes no time)
        } catch (NullPointerException e) {
          logger.warn("skipping bad data: " + record.value());
        }

      }

      if (recordCount > 0) {
        BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        logger.info("Committing offsets...");
        consumer.commitSync();
        logger.info("Offsets have been committed");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    // client.close();

  }
}
