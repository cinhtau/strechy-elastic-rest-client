package net.cinhtau;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import net.cinhtau.config.Configuration;

public class Main {

    static final Logger logger = LogManager.getLogger(Main.class.getName());

    public static void main(String[] args) {

        Configuration configuration = readConfiguration(args[1]);

        try (ElasticsearchConnection es = new ElasticsearchConnection()) {

            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(configuration.getAuth().get("user"), configuration.getAuth().get("password")));

            HttpHost localhost = new HttpHost(configuration.getHost(), configuration.getPort(), "http");
            RestClient restClient = es.connect(credentialsProvider, localhost);

            Response response = restClient.performRequest("GET", "/",
                    Collections.singletonMap("pretty", "true"));
            logger.debug(EntityUtils.toString(response.getEntity()));

            //index a document
            HttpEntity entity = new NStringEntity(
                    "{\n" +
                            "    \"user\" : \"stretch\",\n" +
                            "    \"post_date\" : \"2009-11-15T14:12:12\",\n" +
                            "    \"message\" : \"trying out Elasticsearch\"\n" +
                            "}", ContentType.APPLICATION_JSON);

            Response indexResponse = restClient.performRequest(
                    "PUT",
                    "/twitter/tweet/1",
                    Collections.<String, String>emptyMap(),
                    entity);

            logger.debug("Status {}", indexResponse.getStatusLine());

        } catch (Exception e) {
            logger.error(e);
        }
    }

    private static Configuration readConfiguration(String yamlFileArg) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            return mapper.readValue(new File(yamlFileArg), Configuration.class);
        } catch (IOException e) {
            logger.error(e);
            // return empty config
            return new Configuration();
        }
    }
}