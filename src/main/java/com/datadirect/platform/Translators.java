package com.datadirect.platform;

import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.oracle.OracleExecutionFactory;
import org.teiid.translator.jdbc.sqlserver.SQLServerExecutionFactory;
import org.teiid.translator.odata.ODataExecutionFactory;
import org.teiid.translator.ws.WSExecutionFactory;

/**
 * Created by jmeritt on 4/16/15.
 */
public class Translators {

    public static final String SQLSERVER = "SQL Server";
    public static final String ORACLE = "Oracle";
    public static final String ODATA = "OData";
    public static final String REST = "REST";
    public static final String SOAP = "SOAP";
    static final String TRANSLATOR_MSSQL = "translator-mssql";
    static final String TRANSLATOR_ORACLE = "translator-oracle";
    static final String TRANSLATOR_JDBC = "translator-jdbc";
    static final String TRANSLATOR_REST = "translator-rest";
    static final String TRANSLATOR_ODATA = "translator-odata";


    static void init(EmbeddedServer server) throws TranslatorException {
        ExecutionFactory factory = new SQLServerExecutionFactory();
        factory.start();
        server.addTranslator(TRANSLATOR_MSSQL, factory);

        factory = new WSExecutionFactory();
        factory.start();
        server.addTranslator(TRANSLATOR_REST, factory);

        factory = new OracleExecutionFactory();
        factory.start();
        server.addTranslator(TRANSLATOR_ORACLE, factory);

        factory = new ODataExecutionFactory();
        factory.start();
        server.addTranslator(TRANSLATOR_ODATA, factory);

        factory = new JDBCExecutionFactory();
        factory.start();
        server.addTranslator(TRANSLATOR_JDBC, factory);
    }

    static String translatorForDSType(String dstype) {
        switch (dstype) {
            case SQLSERVER:
                return TRANSLATOR_MSSQL;
            case ORACLE:
                return TRANSLATOR_ORACLE;
            case ODATA:
                return TRANSLATOR_ODATA;
            case REST:
                return TRANSLATOR_REST;
            default:
                return TRANSLATOR_JDBC;
        }
    }
}
