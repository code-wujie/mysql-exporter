package com.bbd.dataplatform.mysql.repository;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.bbd.dataplatform.mysql.util.DbPoolConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

@Service
public class MysqlRepository {

    private DbPoolConnection databasePool = null;
    private static Logger logger = LoggerFactory.getLogger(MysqlRepository.class);

    @Value("${mysql.connection.classname}")
    private String driverClassName;

    @Value("${mysql.connection.filters}")
    private String filters;

    @Value("${mysql.connection.initialsize:25}")
    private String initialSize;

    @Value("${mysql.connection.maxactive:100}")
    private String maxActive;

    @Value("${mysql.connection.maxwait:60000}")
    private String maxWait;

    @Value("${mysql.connection.timeBetweenEvictionRunsMillis:30000}")
    private String timeBetweenEvictionRunsMillis;

    @Value("${mysql.connection.minEvictableIdleTimeMillis:60000}")
    private String minEvictableIdleTimeMillis;

    @Value("${mysql.connection.validationQuery}")
    private String validationQuery;

    @Value("${mysql.connection.testWhileIdle:true}")
    private String testWhileIdle;

    @Value("${mysql.connection.testOnBorrow:false}")
    private String testOnBorrow;

    @Value("${mysql.connection.testOnReturn:false}")
    private String testOnReturn;

    @Value("${mysql.connection.poolPreparedStatements:true}")
    private String poolPreparedStatements;

    @Value("${mysql.connection.maxPoolPreparedStatementPerConnectionSize:500}")
    private String maxPoolPreparedStatementPerConnectionSize;

    private String url;

    private String username;

    private String password;

    public void init(String url, String username, String password) {
        Properties properties = new Properties();//PropertyLoader.loadPropertyFile("/db_server.properties");
        properties.put("driverClassName", driverClassName);
        properties.put("url", url);
        properties.put("username", username);
        properties.put("password", password);
        properties.put("filters", filters);
        properties.put("initialSize", initialSize);
        properties.put("maxActive", maxActive);
        properties.put("maxWait", maxWait);
        properties.put("timeBetweenEvictionRunsMillis", timeBetweenEvictionRunsMillis);
        properties.put("minEvictableIdleTimeMillis", minEvictableIdleTimeMillis);
        properties.put("validationQuery", validationQuery);
        properties.put("testWhileIdle", testWhileIdle);
        properties.put("testOnBorrow", testOnBorrow);
        properties.put("testOnReturn", testOnReturn);
        properties.put("poolPreparedStatements", poolPreparedStatements);
        properties.put("maxPoolPreparedStatementPerConnectionSize", maxPoolPreparedStatementPerConnectionSize);

        databasePool = DbPoolConnection.getInstance(properties);
    }

    public void discardConnection(Connection realConnection) {
        try {
            databasePool.discardConnection(realConnection);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public DruidPooledConnection getConnection() {
        int connectCount = 0;
        while (connectCount < 10) {
            try {
                return databasePool.getConnection();
            } catch (SQLException e) {
                connectCount++;
                try {
                    Thread.sleep(connectCount * 1000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return null;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
