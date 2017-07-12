package net.cinhtau;

import java.io.File;
import java.io.IOException;

import org.apache.http.HttpHost;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import lombok.Getter;
import net.cinhtau.data.Configuration;

public class ElasticsearchConnection implements AutoCloseable {

    static final Logger logger = LogManager.getLogger(ElasticsearchConnection.class.getName());

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

    public Configuration readConfiguration(String yamlFileArg) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            ClassLoader classLoader = Main.class.getClassLoader();
            return mapper.readValue(new File(classLoader.getResource(yamlFileArg).getFile()), Configuration.class);
        } catch (IOException e) {
            logger.error(e);
            // return empty config
            return new Configuration();
        }
    }
}