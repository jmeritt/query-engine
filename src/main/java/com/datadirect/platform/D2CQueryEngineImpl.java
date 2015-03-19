package com.datadirect.platform;

import com.datadirect.util.XMLUtil;
import com.ddtek.jdbcx.ddcloud.DDCloudDataSource;
import org.teiid.resource.adapter.ws.WSManagedConnectionFactory;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.JDBCExecutionFactory;
import org.teiid.translator.jdbc.oracle.OracleExecutionFactory;
import org.teiid.translator.jdbc.sqlserver.SQLServerExecutionFactory;
import org.teiid.translator.ws.WSExecutionFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jmeritt on 3/17/15.
 */
public class D2CQueryEngineImpl extends QueryEngineImpl {

    public static final String TRANSLATOR_MSSQL = "translator-mssql";
    public static final String TRANSLATOR_ORACLE = "translator-oracle";
    public static final String TRANSLATOR_JDBC = "translator-jdbc";
    private String m_d2cUser;
    private String m_d2cPassword;

    public D2CQueryEngineImpl(String username, String password) {
        m_d2cUser = username;
        m_d2cPassword = password;
    }

    public D2CQueryEngineImpl(String localhost, int port, String username, String password) {
        super(localhost, port);
        m_d2cUser = username;
        m_d2cPassword = password;
    }

    @Override
    protected void initModels() throws SQLException {
        try {
            ExecutionFactory factory = new WSExecutionFactory();
            factory.start();
            m_server.addTranslator("translator-rest", factory);

            WSManagedConnectionFactory managedconnectionFactory = new WSManagedConnectionFactory();
            managedconnectionFactory.setAuthUserName(m_d2cUser);
            managedconnectionFactory.setAuthPassword(m_d2cPassword);
            managedconnectionFactory.setSecurityType(WSManagedConnectionFactory.SecurityType.HTTPBasic.name());
            m_server.addConnectionFactory("java:/MetadataRESTWebSvcSource", managedconnectionFactory.createConnectionFactory());

            m_server.deployVDB(Thread.currentThread().getContextClassLoader().getResourceAsStream("META-INF/d2cmetadata-vdb.xml"));

            Document vdbDoc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
            Element vdb = vdbDoc.createElement("vdb");
            vdb.setAttribute("name", "D2CDatasources");
            vdb.setAttribute("version", "1");
            vdbDoc.appendChild(vdb);

            Map<String, String> translators = new HashMap<String, String>();
            Connection conn = getConnection("D2CMetadata", null);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select * from Datasources.DatasourcesMetadata;");
            while (rs.next()) {
                String name = rs.getString("name");

                Element model = vdbDoc.createElement("model");
                model.setAttribute("name", name);
                Element property = vdbDoc.createElement("property");
                property.setAttribute("name", "importer.useFullSchemaName");
                property.setAttribute("value", "true");
                model.appendChild(property);


                String datasource = registerD2CDataSource(name);
                String dstype = rs.getString("type");
                if (!translators.containsKey(dstype))
                    translators.put(dstype, registerTranslatorFor(dstype));
                String translator = translators.get(dstype);
                Element source = vdbDoc.createElement("source");
                source.setAttribute("name", name + "-source");
                source.setAttribute("translator-name", translator);
                source.setAttribute("connection-jndi-name", datasource);
                model.appendChild(source);
                vdb.appendChild(model);
            }
            m_server.deployVDB(XMLUtil.docToInputStream(vdbDoc));
            rs.close();
            stmt.close();
            conn.close();
        } catch (SQLException se) {
            throw se;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    private String registerTranslatorFor(String dstype) throws TranslatorException {
        ExecutionFactory factory = null;
        switch (dstype) {
            case "SQL Server":
                factory = new SQLServerExecutionFactory();
                factory.start();
                m_server.addTranslator(TRANSLATOR_MSSQL, factory);
                return TRANSLATOR_MSSQL;
            case "Oracle":
                factory = new OracleExecutionFactory();
                factory.start();
                m_server.addTranslator(TRANSLATOR_ORACLE, factory);
                return TRANSLATOR_ORACLE;
            default:
                factory = new JDBCExecutionFactory();
                factory.start();
                m_server.addTranslator(TRANSLATOR_JDBC, factory);
                return TRANSLATOR_JDBC;
        }
    }

    private String registerD2CDataSource(String name) {
        DDCloudDataSource d2cDs = new DDCloudDataSource();
        d2cDs.setUser(m_d2cUser);
        d2cDs.setPassword(m_d2cPassword);
        d2cDs.setDatabaseName(name);
        String boundName = String.format("java:/%s", name);
        m_server.addConnectionFactory(boundName, d2cDs);
        return boundName;
    }

}
