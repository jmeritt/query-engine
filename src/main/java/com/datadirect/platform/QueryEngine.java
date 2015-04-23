package com.datadirect.platform;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by jmeritt on 3/17/15.
 */
public abstract class QueryEngine {

    static QueryEngine create(String localhost, int basePort, String username, String password) {
        return new D2CQueryEngineImpl(localhost, basePort, username, password);
    }

    abstract void start() throws SQLException;

    abstract void stop() throws SQLException;

    abstract List<DataSource> allDataSources() throws SQLException;

    abstract void virtualize(List<DataSource> datasources) throws SQLException;

    abstract Connection getConnection() throws SQLException;
    
    
    
}
