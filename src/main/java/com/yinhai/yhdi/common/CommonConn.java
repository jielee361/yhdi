package com.yinhai.yhdi.common;

import org.apache.hadoop.fs.PathIsDirectoryException;

import java.beans.PropertyVetoException;
import java.sql.*;
import java.util.Properties;

public class CommonConn {
    private static final String oraDriver = "oracle.jdbc.driver.OracleDriver";
    private static final String hiveDriver = "org.apache.hive.jdbc.HiveDriver";
    private static final String gpDriver = "com.pivotal.jdbc.GreenplumDriver";
    //    //String oraUrl = "jdbc:oracle:thin:@192.168.140.129:1521/orcl";
    //    //String hiveUrl = "jdbc:hive2://hostip:10016/default"
    //String url = "jdbc:postgresql://192.168.26.220:25432/template1"; org.postgresql.Driver
    //jdbc:pivotal:greenplum://192.168.26.220:25432;DatabaseName=template1
    public static synchronized Connection getOraConnection(String url, String username, String passwd) throws Exception {
        try {
            Class.forName(oraDriver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException("加载ORACLE驱动失败！");
        }
        Connection conn = DriverManager.getConnection(url, username, passwd);
        return conn;

    }

    public static Connection getHiveConnection(String url,String username,String passwd) throws SQLException {
        try {
            Class.forName(hiveDriver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Connection conn = DriverManager.getConnection(url, username, passwd);
        return conn;

    }

    /**
     * 获取GP连接和获取ORACLE连接要加同步，否则两个获取连接的线程都是会卡在：Class.forName(hiveDriver);
     * @param url
     * @param username
     * @param passwd
     * @return
     * @throws SQLException
     */
    public static synchronized Connection getGpconnection(String url,String username,String passwd) throws SQLException {
        try {
            Class.forName(gpDriver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Properties props = new Properties();
        props.setProperty("user",username);
        props.setProperty("password",passwd);
        props.setProperty("ssl","false");
        Connection conn = DriverManager.getConnection(url, props);
        return conn;
    }



    public static Connection getOraPoolConn(String url,String username,String passwd)
            throws PropertyVetoException, SQLException {
        return JdbcPoolUtil.getOraPoolConn(oraDriver,url,username,passwd);
    }

    //关闭连接
    public static void closeConnection(Connection conn){
        if(conn!=null){
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    //关闭连接	ps
    public static void closePS(PreparedStatement ps){
        if(ps!=null){
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    //关闭连接	st
    public static void closeSt(Statement st){
        if(st!=null){
            try {
                st.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    //关闭连接	rs
    public static void closeRS(ResultSet rs){
        if(rs!=null){
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
