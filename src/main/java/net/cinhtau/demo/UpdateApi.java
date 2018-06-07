package net.cinhtau.demo;

import net.cinhtau.data.Configuration;
import net.cinhtau.util.ConnectionHelper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class UpdateApi {
    private static final Configuration CONFIG = ConnectionHelper.readConfiguration("config.yml");

    private static final Logger LOGGER = LogManager.getLogger(UpdateApi.class);

    private static RestClient REST_CLIENT;
    private static RestHighLevelClient CLIENT;

    public static void main(String[] args) throws IOException {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

        Objects.requireNonNull(CONFIG.getAuth(), "no auth data provided");
        String user = CONFIG.getAuth().get("user");
        String password = CONFIG.getAuth().get("password");

        String host = CONFIG.getHost();
        int port = CONFIG.getPort();

        user = (user != null) ? user : "elastic";
        password = (password != null) ? password : "changeme";
        host = (host != null) ? host : "localhost";
        port = (port > 0) ? port : 9200;

        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(user, password));

        HttpHost localhost = new HttpHost(host, port, "http");
        RestClientBuilder builder = RestClient.builder(localhost).
                setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider))
                .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                        .setConnectTimeout(5000)
                        .setSocketTimeout(60000))
                .setMaxRetryTimeoutMillis(60000);
        CLIENT = new RestHighLevelClient(builder);
        try {
            createDocument();
            updateDocument();

        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.CONFLICT) {
                LOGGER.error(e);
            }
        } catch (IOException e) {
            LOGGER.error(e);
        } finally {
            CLIENT.close();
        }
    }

    /**
     * demonstrate update with merge
     */
    private static boolean updateDocument() throws IOException {
        boolean ok = false;

        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("updated", new Date());
        jsonMap.put("reason", "daily update");
        jsonMap.put("amount", 42);
        UpdateRequest request = new UpdateRequest("posts", "doc", "1").doc(jsonMap);

        try {
            UpdateResponse updateResponse = CLIENT.update(request);
            String index = updateResponse.getIndex();
            String type = updateResponse.getType();
            String id = updateResponse.getId();
            long version = updateResponse.getVersion();

            LOGGER.debug("Response: {}, {}, {}, {}", index, type, id, version);
            LOGGER.debug("Rest Response: {}", updateResponse.status());

            if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {
                ok = true;
            } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                ok = true;
            }
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.CONFLICT) {
                LOGGER.error("Conflict {}", e);
            }
            if (e.status() == RestStatus.NOT_FOUND) {
                LOGGER.error("Document not found");
            }
        }

        return ok;
    }

    /**
     * Create a new document
     */
    private static boolean createDocument() throws IOException {
        boolean ok = false;

        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("user", "le-mapper");
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "trying out Elasticsearch");
        IndexRequest indexRequest = new IndexRequest("posts", "doc", "1").source(jsonMap);

        IndexResponse indexResponse = CLIENT.index(indexRequest);

        String index = indexResponse.getIndex();
        String type = indexResponse.getType();
        String id = indexResponse.getId();
        long version = indexResponse.getVersion();

        LOGGER.debug("Response: {}, {}, {}, {}", index, type, id, version);
        LOGGER.debug("Rest Response: {}", indexResponse.status());

        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
            ok = true;
        } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            ok = true;
        }
        ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
            LOGGER.debug("Shards {}", shardInfo.getTotal());
        }
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                String reason = failure.reason();
                LOGGER.warn("Failure reason: {}", reason);
            }
        }

        return ok;
    }
}
