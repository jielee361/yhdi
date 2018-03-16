package com.yinhai.yhdi.common;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;

public class JdbcPoolUtil {
    private static ComboPooledDataSource oracleDataSource;
    private static ComboPooledDataSource hiveDataSource;
    private static Object obj = new Object();
    public static  ComboPooledDataSource initC3p0DataSource(
            String driverName,String url,String username,String password) throws PropertyVetoException {

        ComboPooledDataSource dataSource = new ComboPooledDataSource();
        dataSource.setDriverClass(driverName);
        dataSource.setJdbcUrl(url);
        dataSource.setUser(username);
        dataSource.setPassword(password);
        dataSource.setInitialPoolSize(1);
        dataSource.setMaxPoolSize(10);
        dataSource.setMinPoolSize(1);
        dataSource.setMaxIdleTime(30);//最大空闲时间,30秒内未使用则连接被丢弃
        dataSource.setAcquireIncrement(1);//每次新申请个数。
        dataSource.setAcquireRetryAttempts(10);//申请失败后重试次数
        dataSource.setAcquireRetryDelay(5000);//每次重申请间隔
        //当连接池用完时客户端调用getConnection()后等待获取新连接的时间，超时后将抛出
        //SQLException,如设为0则无限期等待
        dataSource.setCheckoutTimeout(1000);
        dataSource.setIdleConnectionTestPeriod(60);//每60秒检查所有连接池中的空闲连接
        return dataSource;

    }

    public static Connection getConn(String driverName,String url,String username,String password)
            throws PropertyVetoException, SQLException {
        ComboPooledDataSource dataSource;
        if (driverName.contains("oracle")) {
            if (oracleDataSource == null) {
                synchronized (obj) {
                    if (oracleDataSource == null) {
                        oracleDataSource = initC3p0DataSource(driverName,url,username,password);
                    }
                }
            }
            return oracleDataSource.getConnection();
        }else if (driverName.contains("hive")) {
            if (hiveDataSource == null) {
                synchronized (obj) {
                    if (hiveDataSource == null) {
                        hiveDataSource = initC3p0DataSource(driverName,url,username,password);
                    }
                }
            }
            return hiveDataSource.getConnection();
        }else {
            throw new RuntimeException("不识别的驱动类："+driverName);
        }

    }
}
