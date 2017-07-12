package net.cinhtau.util;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import net.cinhtau.Main;
import net.cinhtau.data.Configuration;

public class ConnectionHelper {

    private static final Logger logger = LogManager.getLogger(ConnectionHelper.class.getName());

    private ConnectionHelper() {
        //nth
    }

    /**
     * Reads configuration in yaml format
     * @param yamlFileArg
     * @return
     */
    public static Configuration readConfiguration(String yamlFileArg) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            ClassLoader classLoader = Main.class.getClassLoader();
            return mapper.readValue(new File(classLoader.getResource(yamlFileArg).getFile()), Configuration.class);
        } catch (IOException e) {
            logger.error(e);
            // return empty config
            return null;
        }
    }
}
