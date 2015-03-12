package com.datadirect.platform;


import com.ddtek.jdbcx.ddcloud.DDCloudDataSource;
import org.teiid.core.util.StringUtil;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.jdbc.JDBCExecutionFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class App {

    private static final Logger logger = Logger.getLogger("org.teiid");

    public static void main(String[] args) throws Exception {

        initLogging();
        
        EmbeddedServer server = new EmbeddedServer();
        EmbeddedConfiguration config = new EmbeddedConfiguration();
        config.setUseDisk(true);
        server.start(config);

        JDBCExecutionFactory factory = new JDBCExecutionFactory();
        server.addTranslator("translator-d2c", factory);

        server.addConnectionFactory("java:/sfdc-ds", initD2C());

        server.deployVDB(Thread.currentThread().getContextClassLoader().getResourceAsStream("META-INF/customer-vdb.xml"));

        Connection conn = server.getDriver().connect("jdbc:teiid:Customer360", null);
        queryModel(conn);
        conn.close();
        server.stop();
    }

    private static void initLogging() {
        logger.setLevel(Level.FINE);
        ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(Level.FINE);
        logger.addHandler(handler);
        LogManager.setLogListener(new JavaLogger());
        logger.fine("Checking LogManager configuration");
        logger.fine("Should log " + LogConstants.CTX_RUNTIME + " INFO:" + LogManager.isMessageToBeRecorded(LogConstants.CTX_RUNTIME, MessageLevel.INFO));
        logger.fine("Should log " + LogConstants.CTX_RUNTIME + " CRITICAL:" + LogManager.isMessageToBeRecorded(LogConstants.CTX_RUNTIME, MessageLevel.CRITICAL));
        logger.fine("Should log " + LogConstants.CTX_RUNTIME + " DETAIL:" + LogManager.isMessageToBeRecorded(LogConstants.CTX_RUNTIME, MessageLevel.DETAIL));
        logger.fine("Should log " + LogConstants.CTX_RUNTIME + " TRACE:" + LogManager.isMessageToBeRecorded(LogConstants.CTX_RUNTIME, MessageLevel.TRACE));
    }

    private static void queryModel(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select * from CustomerView limit 1000");
        System.out.println("*************************");
        System.out.println("Name, Title, Company, Country, Zip, Industry, Ticker, Status");
        while(rs.next())
        {
            StringBuilder buff = new StringBuilder(rs.getString("Name")).append(", ");
            buff.append(rs.getString("Title")).append(", ");
            buff.append(rs.getString("Company")).append(", ");
            buff.append(rs.getString("Country")).append(", ");
            buff.append(rs.getString("Zip")).append(", ");
            buff.append(rs.getString("Industry")).append(", ");
            buff.append(rs.getString("Ticker")).append(", ");
            buff.append(rs.getString("Status"));
            System.out.println(buff.toString());
        }
        stmt.close();
    }

    private static DataSource initD2C() throws Exception {


        DDCloudDataSource d2cDs = new DDCloudDataSource();
        d2cDs.setUser("jmeritt");
        d2cDs.setPassword("7ju$u7kJ");
        d2cDs.setDatabaseName("SFDC");
        return d2cDs;
        
        /*// Set up data source reference data for naming context:
        // ----------------------------------------------------
        // Create a pooling manager's class instance that implements
        // the interface DataSource
        PooledConnectionDataSource ds = new PooledConnectionDataSource();

        ds.setDescription("D2C Connection to Salesforce");
        
        ds.setDataSourceName("java:/sfdc-ds", d2cDs);

        // The pool manager will be initiated with 5 physical connections
        ds.setInitialPoolSize(5);


        // The pool maintenance thread will make sure that there are 5
        // physical connections available
        ds.setMinPoolSize(5);

        // The pool maintenance thread will check that there are no more
        // than 10 physical connections available
        ds.setMaxPoolSize(10);

        // The pool maintenance thread will wake up and check the pool
        // every 20 seconds
        ds.setPropertyCycle(20);

        // The pool maintenance thread will remove physical connections
        // that are inactive for more than 300 seconds
        ds.setMaxIdleTime(300);

        // Set tracing off since we choose not to see output listing 
        // of activities on a connection
        ds.setTracing(false);
        return ds;*/
    }

    private static class JavaLogger implements org.teiid.logging.Logger {

        private ConcurrentHashMap<String, Logger> loggers = new ConcurrentHashMap<String, Logger>();

        @Override
        public boolean isEnabled(String context, int msgLevel) {
            Logger logger = getLogger(context);

            Level javaLevel = convertLevel(msgLevel);
            return logger.isLoggable(javaLevel);
        }

        private Logger getLogger(String context) {
            Logger logger = loggers.get(context);
            if (logger == null) {
                logger = Logger.getLogger(context);
                loggers.put(context, logger);
            }
            return logger;
        }

        public void log(int level, String context, Object... msg) {
            log(level, context, null, msg);
        }

        public void log(int level, String context, Throwable t, Object... msg) {
            Logger logger = getLogger(context);

            Level javaLevel = convertLevel(level);

            if (msg.length == 0) {
                logger.log(javaLevel, null, t);
            } else if (msg.length == 1 && !(msg[0] instanceof String)) {
                String msgStr = StringUtil.toString(msg, " ", false); //$NON-NLS-1$
                LogRecord record = new LogRecord(javaLevel, msgStr);
                record.setParameters(msg);
                record.setThrown(t);
                record.setLoggerName(context);
                logger.log(record);
            } else {
                logger.log(javaLevel, StringUtil.toString(msg, " ", false), t); //$NON-NLS-1$
            }
        }

        public Level convertLevel(int level) {
            switch (level) {
                case MessageLevel.CRITICAL:
                case MessageLevel.ERROR:
                    return Level.SEVERE;
                case MessageLevel.WARNING:
                    return Level.WARNING;
                case MessageLevel.INFO:
                    return Level.FINE;
                case MessageLevel.DETAIL:
                    return Level.FINER;
                case MessageLevel.TRACE:
                    return Level.FINEST;
            }
            return Level.ALL;
        }

        public void shutdown() {
        }

        @Override
        public void putMdc(String key, String val) {

        }

        @Override
        public void removeMdc(String key) {

        }

    }
}
