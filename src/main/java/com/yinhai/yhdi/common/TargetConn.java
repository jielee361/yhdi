package com.yinhai.yhdi.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class TargetConn {
    private static final String gpDriver = "org.postgresql.Driver";
    //String url = "jdbc:postgresql://192.168.26.220:25432/template1";
    public static Connection getGpconnection(String url, String username, String passwd) throws SQLException {
        try {
            Class.forName(gpDriver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Properties props = new Properties();
        props.setProperty("user",username);
        props.setProperty("password",passwd);
        //props.setProperty("ssl","false");
        Connection conn = DriverManager.getConnection(url, props);
        return conn;
    }
}
