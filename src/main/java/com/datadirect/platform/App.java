package com.datadirect.platform;


import java.util.ArrayList;
import java.util.List;

public class App {

    public static void main(String[] args) throws Exception {
        QueryEngine engine = new D2CQueryEngineImpl("localhost", 31000, "jmeritt", "7ju$u7kJ");
        engine.start();
        List<DataSource> dbs = engine.allDataSources();
        List<DataSource> virts = new ArrayList<>();
        for (DataSource ds : dbs) {
            switch (ds.getType()) {
                case "Salesforce":
                case "Dynamics CRM":
                    ds.addProperty("importer.importKeys", "false");
                    virts.add(ds);
                default:
                    continue;
            }
        }
        DataSource odata = new DataSource();
        odata.setName("Northwind");
        odata.setType(Translators.ODATA);
        odata.setEndpoint("http://services.odata.org/V2/Northwind/Northwind.svc/");
        virts.add(odata);
        engine.virtualize(virts);
        System.in.read();
        engine.stop();
        System.exit(0);
    }
}
