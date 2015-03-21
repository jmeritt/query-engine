package com.datadirect.platform;

import com.datadirect.util.SLFLogger;
import org.teiid.logging.LogManager;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.transport.SocketConfiguration;
import org.teiid.transport.WireProtocol;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by jmeritt on 3/17/15.
 */
public abstract class QueryEngineImpl implements QueryEngine {


    protected EmbeddedServer m_server;
    protected boolean m_remoteAccess;
    protected String m_hostname;
    private int m_port;

    protected QueryEngineImpl(String hostname, int port) {
        m_remoteAccess = true;
        m_hostname = hostname;
        m_port = port;
    }

    protected QueryEngineImpl() {
        m_remoteAccess = false;
    }

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

    public Connection getConnection(String vdbName, Properties props) throws SQLException {
        return m_server.getDriver().connect("jdbc:teiid:" + vdbName, props);
    }

    private void initTeiidLogging() {
        LogManager.setLogListener(new SLFLogger());
    }

    protected abstract void init() throws SQLException;

    protected void initTransports(EmbeddedConfiguration config) {
        if (m_remoteAccess) {
            SocketConfiguration s = new SocketConfiguration();
            s.setBindAddress(m_hostname);
            s.setPortNumber(m_port);
            s.setProtocol(WireProtocol.pg);
            config.addTransport(s);
        }
    }

    @Override
    public void stop() {
        m_server.stop();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return getConnection(getVDBName(), null);
    }

    protected abstract String getVDBName();


}
