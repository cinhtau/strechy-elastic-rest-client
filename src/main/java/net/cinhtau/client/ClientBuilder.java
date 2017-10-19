package net.cinhtau.client;

import org.apache.http.HttpHost;
import org.apache.http.client.CredentialsProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;

public class ClientBuilder {

    private static final Logger LOGGER = LogManager.getLogger(ClientBuilder.class);

    private ClientBuilder() {
    }

    public static RestClient connect(final CredentialsProvider credentialsProvider, HttpHost... httpHosts) {

        for (HttpHost host : httpHosts) {
            LOGGER.info("Using host: {}", host.getHostName());
        }

        return RestClient.builder(httpHosts).
                setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider))
                .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                        .setConnectTimeout(5000)
                        .setSocketTimeout(60000))
                .setMaxRetryTimeoutMillis(60000)
                .build();
    }
}