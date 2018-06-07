package net.cinhtau.data;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

public class Configuration {
    /**
     * The elasticsearch host to connect with, IP address is also ok
     */
    @Getter @Setter
    private String host;
    /**
     * The http port to use
     */
    @Getter @Setter
    private int port;
    /**
     * Contains authentication data, only needed if elasticsearch is secured
     * either with x-pack or nginx basic auth
     */
    @Getter @Setter
    private Map<String, String> auth;

    @Getter
    @Setter
    private String trustStorePass;
}
