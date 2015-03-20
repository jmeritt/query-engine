package com.datadirect.platform;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by jmeritt on 3/17/15.
 */
public interface QueryEngine {

    void start() throws SQLException;
    void stop() throws SQLException;

    List<DataSource> allDataSources() throws SQLException;

    void virtualize(List<DataSource> datasources) throws SQLException;

    Connection getConnection() throws SQLException;
    
    
    
}
