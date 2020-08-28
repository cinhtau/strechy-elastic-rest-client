package net.cinhtau.data;

import java.util.HashMap;
import java.util.Map;

public class Configuration {

    /**
     * The elasticsearch host to connect with, IP address is also ok
     */
    private String host;
    /**
     * The http port to use
     */
    private int port;

    /**
     * Contains authentication data, only needed if elasticsearch is secured
     * either with x-pack or nginx basic auth
     */
    private Map<String, String> auth = new HashMap<>();

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Map<String, String> getAuth() {
        return auth;
    }

    public void setAuth(Map<String, String> auth) {
        this.auth = auth;
    }
}
