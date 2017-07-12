package net.cinhtau.util;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;

import org.junit.Test;

import net.cinhtau.data.Configuration;

public class ConnectionHelperTest {

    /**
     * read configuration from yaml
     *
     * @throws Exception
     */
    @Test
    public void readConfiguration() throws Exception {
        Configuration actual = ConnectionHelper.readConfiguration("config.yml");
        assertNotNull("configuration object was created", actual);
        assertThat("host correctly retrieved", actual.getHost(), is("localhost"));
        assertThat("default port retrieved", actual.getPort(), is(9200));
        assertThat("authentication is not empty", new ArrayList<>(actual.getAuth().values()), hasSize(2));
        assertThat("user is elastic", actual.getAuth(), hasEntry(equalTo("user"), equalTo("elastic")));
        assertThat("password is heart", actual.getAuth(), hasEntry(equalTo("password"), equalTo("heart")));
    }
}