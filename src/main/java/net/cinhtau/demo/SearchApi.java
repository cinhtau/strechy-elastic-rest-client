package net.cinhtau.demo;

import net.cinhtau.data.Configuration;
import net.cinhtau.util.ConnectionHelper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SearchApi {

    private static final Configuration CONFIG = ConnectionHelper.readConfiguration("config.yml");

    private static final Logger LOGGER = LogManager.getLogger(SearchApi.class);

    private static RestClient REST_CLIENT;
    private static RestHighLevelClient CLIENT;

    public static void main(String[] args) throws IOException {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

        Objects.requireNonNull(CONFIG.getAuth(), "no auth data provided");
        String user = CONFIG.getAuth().get("user");
        String password = CONFIG.getAuth().get("password");

        String host = CONFIG.getHost();
        int port = CONFIG.getPort();
        String trustStorePass = CONFIG.getTrustStorePass();

        user = (user != null) ? user : "elastic";
        password = (password != null) ? password : "changeme";
        host = (host != null) ? host : "localhost";
        port = (port > 0) ? port : 9200;

        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(user, password));

        HttpHost elasticHost = new HttpHost(host, port, "https");

        try {
            Path keyStorePath = Paths.get(ClassLoader.getSystemResource("truststore.jks").toURI());


            KeyStore truststore = KeyStore.getInstance("jks");
            try (InputStream is = Files.newInputStream(keyStorePath)) {
                truststore.load(is, trustStorePass.toCharArray());
            }
            SSLContextBuilder sslBuilder = SSLContexts.custom().loadTrustMaterial(truststore, null);
            final SSLContext sslContext = sslBuilder.build();

            RestClientBuilder builder = RestClient.builder(elasticHost)
                    .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setSSLContext(sslContext));

            builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
            builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                    .setConnectTimeout(5000)
                    .setSocketTimeout(60000))
                    .setMaxRetryTimeoutMillis(60000);

            CLIENT = new RestHighLevelClient(builder);

            queryDocument();
        } catch (IOException | CertificateException | NoSuchAlgorithmException | KeyStoreException | KeyManagementException | URISyntaxException e) {
            LOGGER.error(e);
        } finally {
            CLIENT.close();
        }
    }

    private static void queryDocument() throws IOException {
        SearchRequest searchRequest = new SearchRequest("six-audit-2018.06.06");
        searchRequest.types("doc");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        QueryBuilder queryBuilder = QueryBuilders.matchQuery("request", "MultiSearchRequest");

        // parent aggregation
        DateHistogramAggregationBuilder aggregation = AggregationBuilders.
                dateHistogram("users_over_time").
                field("@timestamp").
                dateHistogramInterval(DateHistogramInterval.hours(1));
        // sub-aggregation
        aggregation.subAggregation(AggregationBuilders.terms("users")
                .field("principal"));

        searchSourceBuilder.query(queryBuilder);
        searchSourceBuilder.aggregation(aggregation);

        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = CLIENT.search(searchRequest);

        RestStatus status = searchResponse.status();

        if (status == RestStatus.OK) {
            Aggregations aggregations = searchResponse.getAggregations();

            Histogram dateHistogram = aggregations.get("users_over_time");

            for (Histogram.Bucket bucket : dateHistogram.getBuckets()) {

                LOGGER.info("Key: {}", bucket.getKeyAsString());

                Map<String, Aggregation> aggregationMap = bucket.getAggregations().asMap();

                Aggregation userAggregration = aggregationMap.get("users");

                List<? extends Terms.Bucket> buckets = ((Terms) userAggregration).getBuckets();
                LOGGER.info("-- {} active users", buckets.size());

                for (Terms.Bucket user : buckets) {
                    LOGGER.info("   User {}", user.getKey());
                }
            }
        }
    }
}

