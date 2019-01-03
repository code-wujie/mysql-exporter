package com.bbd.dataplatform.mysql.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.alibaba.druid.pool.DruidPooledConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;

public class DbPoolConnection {
    private static HashMap<Properties, DbPoolConnection> DbMap = new HashMap<>();
    private DruidDataSource dds = null;

    public DbPoolConnection(Properties properties) {
        try {
            dds = (DruidDataSource) DruidDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static synchronized DbPoolConnection getInstance(Properties properties) {
        DbPoolConnection databasePool;
        if (DbMap.containsKey(properties)) {
            databasePool = DbMap.get(properties);
        } else {
            databasePool = new DbPoolConnection(properties);
            DbMap.put(properties, databasePool);
        }
        return databasePool;
    }

    public void discardConnection(Connection realConnection) throws SQLException {
        dds.discardConnection(realConnection);
    }

    public DruidPooledConnection getConnection() throws SQLException {
        return dds.getConnection();
    }

}