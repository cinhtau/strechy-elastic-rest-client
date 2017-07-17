package net.cinhtau.client;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;

import net.cinhtau.ElasticsearchConnection;
import net.cinhtau.data.Configuration;

public class AsynchronousSender {

    static final Logger logger = LogManager.getLogger(AsynchronousSender.class);

    private final Configuration configuration;

    public AsynchronousSender(final Configuration configuration) {
        this.configuration = configuration;
    }

    public void sendDocuments(List<String> jsonData) {
        try (ElasticsearchConnection es = new ElasticsearchConnection()) {

            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

            String user = this.configuration.getAuth().get("user");
            String password = this.configuration.getAuth().get("password");

            logger.debug("authentication data {}:{}", user, password);

            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(user, password));

            HttpHost localhost = new HttpHost(configuration.getHost(), configuration.getPort(), "http");
            RestClient restClient = es.connect(credentialsProvider, localhost);

            HttpEntity[] entities = new HttpEntity[jsonData.size()];

            for (int i = 0; i < jsonData.size(); i++) {
                entities[i] = new NStringEntity(jsonData.get(i), ContentType.APPLICATION_JSON);
                logger.debug("http entity : {}", entities[i]);
            }

            int numRequests = jsonData.size();
            final CountDownLatch latch = new CountDownLatch(numRequests);

            for (int i = 0; i < numRequests; i++) {
                restClient.performRequestAsync(
                        "PUT",
                        "/vinh/data/" + i,
                        Collections.<String, String>emptyMap(),
                        entities[i],
                        new ResponseListener() {
                            @Override
                            public void onSuccess(Response response) {
                                logger.debug(response);
                                latch.countDown();
                            }

                            @Override
                            public void onFailure(Exception exception) {
                                logger.error("Something went south: {}", exception);
                                latch.countDown();
                            }
                        }
                );
            }

            //wait for all requests to be completed
            latch.await();

        } catch (Exception e) {
            logger.error(e);
        }
    }
}
