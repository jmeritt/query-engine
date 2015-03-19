package com.datadirect.platform;


import java.net.Inet4Address;

public class App {

    public static void main(String[] args) throws Exception {
        QueryEngine engine = new D2CQueryEngineImpl(Inet4Address.getLocalHost().getHostAddress(), 31000, "jmeritt", "7ju$u7kJ");
        engine.start();
        engine.stop();
        System.exit(0);
    }
}
