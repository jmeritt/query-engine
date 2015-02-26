package com.datadirect.platform;


import com.ddtek.jdbcx.ddcloud.DDCloudDataSource;
import com.ddtek.jdbcx.ddcloud.DDCloudDataSource40;
import com.ddtek.pool.PooledConnectionDataSource;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.jdbc.JDBCExecutionFactory;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.PooledConnection;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class App {
    public static void main(String[] args) throws Exception {
        
        Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
        logger.setLevel(Level.ALL);
        
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
        queryModel(conn);
        queryModel(conn);
        conn.close();
        server.stop();
    }

    private static void queryModel(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select * from CustomerView limit 100");
        while(rs.next())
        {
            System.out.print("*");
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
}
