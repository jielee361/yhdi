package com.yinhai.yhdi.common;

import java.beans.PropertyVetoException;
import java.sql.*;

public class CommonConn {
    private static final String oraDriver = "oracle.jdbc.driver.OracleDriver";
    private static final String hiveDriver = "org.apache.hive.jdbc.HiveDriver";
    //String oraUrl = "jdbc:oracle:thin:@192.168.140.129:1521/orcl";
    //String hiveUrl = "jdbc:hive2://hostip:10016/default"
    public static Connection getOraConnection(String url,String username,String passwd) throws Exception {
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
