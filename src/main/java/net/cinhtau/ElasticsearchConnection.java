package net.cinhtau;

import org.apache.http.HttpHost;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import lombok.Getter;

public class ElasticsearchConnection implements AutoCloseable {

    static final Logger logger = LogManager.getLogger(ElasticsearchConnection.class);

    @Getter
    private RestClient restClient;

    public RestClient connect(final CredentialsProvider credentialsProvider, HttpHost... httpHosts) {
        this.restClient = RestClient.builder(httpHosts).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        }).build();
        return this.restClient;
    }

    @Override
    public void close() throws Exception {
        restClient.close();
        logger.debug("Connection closed");
    }
}