package com.datadirect.platform;

import com.datadirect.util.XMLUtil;
import com.ddtek.jdbcx.ddcloud.DDCloudDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teiid.resource.adapter.ws.WSManagedConnectionFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.resource.ResourceException;
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

/**
 * Created by jmeritt on 3/17/15.
 */
class D2CQueryEngineImpl extends QueryEngineImpl {

    static final String D2C_VDB = "D2CVDB";
    private static final Logger LOG = LoggerFactory.getLogger(D2CQueryEngineImpl.class);
    private String m_d2cUser;
    private String m_d2cPassword;
    private int m_version;
    private DocumentBuilder m_builder;
    private List<DataSource> m_virtualizedDatasources;
    private List<DataSource> m_defaultDatasources;

    public D2CQueryEngineImpl(String localhost, int port, String username, String password) {
        super(localhost, port);
        m_d2cUser = username;
        m_d2cPassword = password;
        m_defaultDatasources = m_virtualizedDatasources = initialDatasources();
        try {
            m_builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            LOG.error("Cannot setup XML parser", e);
        }
    }

    private static Object createConnectionFactory(DataSource ds) throws ResourceException {
        switch (ds.getType()) {
            case Translators.REST:
            case Translators.ODATA:
            case Translators.SOAP:
                WSManagedConnectionFactory mcf = new WSManagedConnectionFactory();
                mcf.setAuthUserName(ds.getUsername());
                mcf.setAuthPassword(ds.getPassword());
                mcf.setSecurityType(WSManagedConnectionFactory.SecurityType.HTTPBasic.name());
                mcf.setEndPoint(ds.getEndpoint());
                return mcf.createConnectionFactory();
            case Translators.ORACLE:
            case Translators.SQLSERVER:
            default:
                DDCloudDataSource d2cDs = new DDCloudDataSource();
                d2cDs.setUser(ds.getUsername());
                d2cDs.setPassword(ds.getPassword());
                d2cDs.setDatabaseName(ds.getName());
                return d2cDs;
        }
    }

    @Override
    protected void init() throws SQLException {
        try {
            Translators.init(m_server);
            virtualize();
        } catch (SQLException se) {
            throw se;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    private List<DataSource> initialDatasources() {

        ArrayList l = new ArrayList();
        HashMap<String, String> props = new HashMap();
        String name = "D2CMetadata";
        props.put(DataSource.USERNAME, m_d2cUser);
        props.put(DataSource.PASSWORD, m_d2cPassword);
        props.put(DataSource.VIEWS_DDL, "CREATE  VIEW DatastoresView (id varchar(5), type varchar(128))\n" +
                "            AS SELECT T.id, T.name\n" +
                "\t        FROM\n" +
                "\t\t    (CALL " + name + ".invokeHttp('GET', null, 'https://service.datadirectcloud.com/api/mgmt/datastores', 'TRUE')) AS f, \n" +
                "\t\t    XMLTABLE('/holder/dataStores' PASSING JSONTOXML('holder', f.result) \n" +
                "\t\t        COLUMNS id string PATH 'id/text()', name string PATH 'name/text()')as T;\n" +
                "\t\t    \n" +
                "\t\t    CREATE  VIEW DatasourcesView (id varchar(5), name varchar(128), datastoreid varchar(5))\n" +
                "\t\t    AS SELECT T.id, T.name, T.datastoreid\n" +
                "\t        FROM\n" +
                "\t\t    (CALL " + name + ".invokeHttp('GET', null, 'https://service.datadirectcloud.com/api/mgmt/datasources', 'TRUE')) AS f, \n" +
                "\t\t    XMLTABLE('/holder/dataSources' PASSING JSONTOXML('holder', f.result) \n" +
                "\t\t        COLUMNS id string PATH 'id/text()', name string PATH 'name/text()', datastoreid string PATH 'dataStore/text()')as T;");

        l.add(new DataSource(name, Translators.REST, props));
        return l;
    }

    List<DataSource> loadDatasources() throws SQLException {
        Connection connection = m_server.getDriver().connect(String.format("jdbc:teiid:%s", D2C_VDB), null);
        List<DataSource> datasources = new ArrayList<>();
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT DatasourcesView.name, DatastoresView.type FROM DatasourcesView, DatastoresView WHERE DatasourcesView.datastoreid = DatastoresView.id");
        while (rs.next()) {
            datasources.add(new DataSource(rs.getString("name"), rs.getString("type"), m_d2cUser, m_d2cPassword));
        }
        rs.close();
        stmt.close();
        connection.close();
        return datasources;
    }


    @Override
    protected String getVDBName() {
        return D2C_VDB;
    }


    @Override
    public List<DataSource> allDataSources() throws SQLException {
        List<DataSource> l = loadDatasources();
        l.addAll(m_defaultDatasources);
        return l;
    }


    @Override
    public void virtualize(List<DataSource> datasources) throws SQLException {
        m_virtualizedDatasources = new ArrayList(m_defaultDatasources);
        m_virtualizedDatasources.addAll(datasources);
        virtualize();
    }

    private void virtualize() throws SQLException {
        try {
            Document vdbDoc = m_builder.newDocument();
            Element vdb = vdbDoc.createElement("vdb");
            vdb.setAttribute("name", D2C_VDB);
            vdb.setAttribute("version", String.format("%d", ++m_version));
            vdbDoc.appendChild(vdb);

            for (DataSource ds : m_virtualizedDatasources) {
                for (Element e : ds.buildModels(m_server, vdbDoc, createConnectionFactory(ds)))
                    vdb.appendChild(e);
            }
            m_server.undeployVDB(D2C_VDB);
            m_server.deployVDB(XMLUtil.docToInputStream(vdbDoc));
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

}
