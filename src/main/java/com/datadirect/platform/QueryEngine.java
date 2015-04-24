package com.datadirect.platform;

import com.datadirect.util.SLFLogger;
import org.teiid.logging.LogManager;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.transport.SocketConfiguration;
import org.teiid.transport.WireProtocol;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

/**
 * Created by jmeritt on 3/17/15.
 */
public abstract class QueryEngine {

    protected EmbeddedServer m_server;
    protected String m_hostname;
    protected int m_port;

    public QueryEngine(String hostname, int port) {
        m_port = port;
        m_hostname = hostname;
    }

    public static QueryEngine create(String localhost, int basePort, String username, String password) {
        return new D2CQueryEngineImpl(localhost, basePort, username, password);
    }

    public abstract List<DataSource> allDataSources() throws SQLException;

    public abstract void virtualize(List<DataSource> datasources) throws SQLException;

    public void start() throws SQLException {

        initTeiidLogging();

        m_server = new EmbeddedServer();
        EmbeddedConfiguration config = new EmbeddedConfiguration();
        initTransports(config);
        config.setUseDisk(true);
        config.setTransactionManager(com.arjuna.ats.jta.TransactionManager.transactionManager());
        m_server.start(config);

        init();

    }

    private void initTeiidLogging() {
        LogManager.setLogListener(new SLFLogger());
    }

    protected abstract void init() throws SQLException;

    protected void initTransports(EmbeddedConfiguration config) {
        SocketConfiguration s = new SocketConfiguration();
        s.setBindAddress(m_hostname);
        s.setPortNumber(m_port);
        s.setProtocol(WireProtocol.teiid);
        config.addTransport(s);
        s = new SocketConfiguration();
        s.setBindAddress(m_hostname);
        s.setPortNumber(m_port + 1);
        s.setProtocol(WireProtocol.pg);
        config.addTransport(s);
    }

    public void stop() {
        m_server.stop();
    }

    public Connection getConnection() throws SQLException {
        return getConnection(getVDBName(), null);
    }

    protected abstract String getVDBName();

    public Connection getConnection(String vdbName, Properties props) throws SQLException {
        return m_server.getDriver().connect("jdbc:teiid:" + vdbName, props);
    }
}
