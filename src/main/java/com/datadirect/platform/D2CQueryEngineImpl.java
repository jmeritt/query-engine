package com.datadirect.platform;

import com.datadirect.util.XMLUtil;
import com.ddtek.jdbcx.ddcloud.DDCloudDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teiid.resource.adapter.ws.WSManagedConnectionFactory;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.oracle.OracleExecutionFactory;
import org.teiid.translator.jdbc.postgresql.PostgreSQLExecutionFactory;
import org.teiid.translator.jdbc.sqlserver.SQLServerExecutionFactory;
import org.teiid.translator.ws.WSExecutionFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jmeritt on 3/17/15.
 */
public class D2CQueryEngineImpl extends QueryEngineImpl {

    public static final String TRANSLATOR_MSSQL = "translator-mssql";
    public static final String TRANSLATOR_ORACLE = "translator-oracle";
    public static final String TRANSLATOR_JDBC = "translator-jdbc";
    private static final String D2C_VDB = "D2CVDB";
    private static final Logger LOG = LoggerFactory.getLogger(D2CQueryEngineImpl.class);
    private String m_d2cUser;
    private String m_d2cPassword;
    private int m_version;
    private Connection m_metadataConnection;
    private DocumentBuilder m_builder;

    public D2CQueryEngineImpl(String username, String password) {
        setupVars(username, password);
    }

    public D2CQueryEngineImpl(String localhost, int port, String username, String password) {
        super(localhost, port);
        setupVars(username, password);
    }

    private void setupVars(String username, String password) {
        m_d2cUser = username;
        m_d2cPassword = password;
        try {
            m_builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            LOG.error("Cannot setup XML parser", e);
        }
    }

    @Override
    public void stop() {
        try {
            m_metadataConnection.close();
        } catch (SQLException e) {
            LOG.warn("Issue closing connection during shutdown", e);
        }
        super.stop();
    }

    @Override
    protected String getVDBName() {
        return D2C_VDB;
    }

    @Override
    protected void init() throws SQLException {
        try {
            ExecutionFactory factory = new WSExecutionFactory();
            factory.start();
            m_server.addTranslator("translator-rest", factory);

            WSManagedConnectionFactory mcf = new WSManagedConnectionFactory();
            mcf.setAuthUserName(m_d2cUser);
            mcf.setAuthPassword(m_d2cPassword);
            mcf.setSecurityType(WSManagedConnectionFactory.SecurityType.HTTPBasic.name());
            m_server.addConnectionFactory("java:/MetadataRESTWebSvcSource", mcf.createConnectionFactory());
            m_server.deployVDB(Thread.currentThread().getContextClassLoader().getResourceAsStream("META-INF/d2cmetadata-vdb.xml"));
            m_metadataConnection = getConnection("D2CMetadata", null);
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
                factory = new PostgreSQLExecutionFactory();
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

    @Override
    public List<DataSource> allDataSources() throws SQLException {
        List<DataSource> datasources = new ArrayList<>();
        Statement stmt = m_metadataConnection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT DatasourcesView.name, DatastoresView.type FROM DatasourcesView, DatastoresView WHERE DatasourcesView.datastoreid = DatastoresView.id");
        while (rs.next()) {
            datasources.add(new DataSource(rs.getString("name"), rs.getString("type")));
        }
        rs.close();
        stmt.close();
        return datasources;
    }

    @Override
    public void virtualize(List<DataSource> datasources) throws SQLException {
        try {
            Document vdbDoc = m_builder.newDocument();
            Element vdb = vdbDoc.createElement("vdb");
            vdb.setAttribute("name", D2C_VDB);
            vdb.setAttribute("version", String.format("%d", ++m_version));
            vdbDoc.appendChild(vdb);

            Map<String, String> translators = new HashMap<String, String>();
            for (DataSource ds : datasources) {
                String name = ds.getName();

                Element model = vdbDoc.createElement("model");
                model.setAttribute("name", name);
                Element property = vdbDoc.createElement("property");
                property.setAttribute("name", "importer.useFullSchemaName");
                property.setAttribute("value", "false");
                model.appendChild(property);
                property = vdbDoc.createElement("property");
                property.setAttribute("name", "importer.importKeys");
                property.setAttribute("value", "false");
                model.appendChild(property);


                String datasource = registerD2CDataSource(name);
                String dstype = ds.getType();
                if (!translators.containsKey(dstype))
                    translators.put(dstype, registerTranslatorFor(dstype));
                String translator = translators.get(dstype);
                Element source = vdbDoc.createElement("source");
                source.setAttribute("name", "source-" + name);
                source.setAttribute("translator-name", translator);
                source.setAttribute("connection-jndi-name", datasource);
                model.appendChild(source);
                vdb.appendChild(model);
            }
            m_server.deployVDB(XMLUtil.docToInputStream(vdbDoc));
        } catch (Exception e) {
            throw new SQLException(e);
        }

    }

}
