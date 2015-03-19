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
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

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
        m_server.start(config);

        initModels();

    }

    @Override
    public Connection getConnection(String db, Properties props) throws SQLException {
        return m_server.getDriver().connect("jdbc:teiid:" + db, props);
    }

    private void initTeiidLogging() {
        Logger logger = Logger.getLogger("org.teiid");
        logger.setLevel(Level.FINEST);
        ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(Level.FINEST);
        logger.addHandler(handler);
        LogManager.setLogListener(new SLFLogger());
    }

    protected abstract void initModels() throws SQLException;

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


}
