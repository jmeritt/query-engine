package com.datadirect.platform;

import com.arjuna.ats.arjuna.common.ObjectStoreEnvironmentBean;
import com.arjuna.ats.arjuna.common.arjPropertyManager;
import com.arjuna.common.internal.util.propertyservice.BeanPopulator;
import com.datadirect.util.SLFLogger;
import org.teiid.logging.LogManager;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.transport.SocketConfiguration;
import org.teiid.transport.WireProtocol;

import javax.transaction.TransactionManager;
import java.io.File;
import java.security.AccessController;
import java.security.PrivilegedAction;
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

    private static TransactionManager getTransactionManager() {

        arjPropertyManager.getCoreEnvironmentBean().setNodeIdentifier("1");
        arjPropertyManager.getCoreEnvironmentBean().setSocketProcessIdPort(0);
        arjPropertyManager.getCoreEnvironmentBean().setSocketProcessIdMaxPorts(10);

        arjPropertyManager.getCoordinatorEnvironmentBean().setEnableStatistics(false);
        arjPropertyManager.getCoordinatorEnvironmentBean().setDefaultTimeout(300);
        arjPropertyManager.getCoordinatorEnvironmentBean().setTransactionStatusManagerEnable(false);
        arjPropertyManager.getCoordinatorEnvironmentBean().setTxReaperTimeout(120000);

        String storeDir = getStoreDir();

        arjPropertyManager.getObjectStoreEnvironmentBean().setObjectStoreDir(storeDir);
        BeanPopulator.getNamedInstance(ObjectStoreEnvironmentBean.class, "communicationStore").setObjectStoreDir(storeDir); //$NON-NLS-1$

        return com.arjuna.ats.jta.TransactionManager.transactionManager();
    }

    private static String getStoreDir() {
        String defDir = getSystemProperty("user.home") + File.separator + ".teiid/embedded/data"; //$NON-NLS-1$ //$NON-NLS-2$
        return getSystemProperty("teiid.embedded.txStoreDir", defDir);
    }

    private static String getSystemProperty(final String name, final String value) {
        return AccessController.doPrivileged(new PrivilegedAction<String>() {

            @Override
            public String run() {
                return System.getProperty(name, value);
            }
        });
    }

    private static String getSystemProperty(final String name) {
        return AccessController.doPrivileged(new PrivilegedAction<String>() {

            @Override
            public String run() {
                return System.getProperty(name);
            }
        });
    }

    public abstract List<DataSource> allDataSources() throws SQLException;

    public abstract void virtualize(List<DataSource> datasources) throws SQLException;

    public void start() throws SQLException {

        initTeiidLogging();

        m_server = new EmbeddedServer();
        EmbeddedConfiguration config = new EmbeddedConfiguration();
        initTransports(config);
        config.setUseDisk(true);
        config.setTransactionManager(getTransactionManager());
        //config.setSecurityHelper(new SecurityHelper());
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
