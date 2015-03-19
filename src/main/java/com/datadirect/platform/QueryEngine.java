package com.datadirect.platform;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by jmeritt on 3/17/15.
 */
public interface QueryEngine {

    void start() throws SQLException;

    Connection getConnection(String db, Properties props) throws SQLException;

    void stop() throws SQLException;
}
