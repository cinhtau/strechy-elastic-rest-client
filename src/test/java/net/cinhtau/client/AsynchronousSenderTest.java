package net.cinhtau.client;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.Ignore;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

import net.cinhtau.ElasticsearchConnection;
import net.cinhtau.data.Configuration;
import net.cinhtau.data.CountResponse;
import net.cinhtau.data.Person;
import net.cinhtau.util.ConnectionHelper;

public class AsynchronousSenderTest {

    static final Logger logger = LogManager.getLogger(AsynchronousSenderTest.class);

    static final Configuration CONFIG = ConnectionHelper.readConfiguration("config.yml");

    @Ignore("Needs valid active endpoint for testing")
    @Test
    public void sendDocuments() throws Exception {
        //given

        //--data
        GsonBuilder gsonBuilder = new GsonBuilder();
        Gson gson = gsonBuilder.create();

        File input = new File(this.getClass().getClassLoader().getResource("data.json").toURI());
        JsonReader reader = new JsonReader(new FileReader(input));

        Type collectionType = new TypeToken<Collection<Person>>() {
        }.getType();
        Collection<Person> data = gson.fromJson(reader, collectionType);

        List<String> inputList = new ArrayList<>(data.size());

        for (Person person : data) {
            logger.debug("Deserialized: {}", person);
            String json = gson.toJson(person);
            logger.debug("Serialize: {}", json);
            inputList.add(gson.toJson(person));
        }

        // when
        AsynchronousSender sender = new AsynchronousSender(CONFIG);
        sender.sendDocuments(inputList);
        // then
        assertThat("five documents created", readDocumentCount(), is(5));

    }

    /**
     * Read and examine response of Count Document API
     * @return counted documents
     */
    private int readDocumentCount() {
        try (ElasticsearchConnection es = new ElasticsearchConnection()) {

            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

            String user = CONFIG.getAuth().get("user");
            String password = CONFIG.getAuth().get("password");

            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(user, password));

            HttpHost localhost = new HttpHost(CONFIG.getHost(), CONFIG.getPort(), "http");
            RestClient restClient = es.connect(credentialsProvider, localhost);

            Response response = restClient.performRequest("GET", "/vinh/data/_count",
                    Collections.singletonMap("pretty", "true"));

            logger.debug("ResponseCode: {}", response.getStatusLine());
            logger.debug("Content: {}", response.getEntity());

            InputStream is = response.getEntity().getContent();

            ByteArrayOutputStream result = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int length;
            while ((length = is.read(buffer)) != -1) {
                result.write(buffer, 0, length);
            }

            String jsonString = result.toString(StandardCharsets.UTF_8.name());
            logger.debug("Response Content: {}", jsonString);

            Gson gson = new Gson();
            CountResponse countResponse = gson.fromJson(jsonString, CountResponse.class);

            return countResponse.getCount();
        } catch (Exception e) {
            logger.error(e);
            return 0;
        }
    }
}