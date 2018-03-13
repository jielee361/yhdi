package com.yinhai.yhdi.increment;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TableUpdateStat {
    public IcrmtTable getLastUpdate(Connection carbonConn,String carbonTable) throws SQLException {
        Statement st = carbonConn.createStatement();
        String sql = "select * from (select * from carbon_odi_stat where carbon_table='"+
                carbonTable + "' order by end_time desc) a limit 1";
        ResultSet rs = st.executeQuery(sql);
        IcrmtTable itable = new IcrmtTable();
        while (rs.next()) {
            itable.setCarbonTable(carbonTable);
            itable.setHiveTable(rs.getString(2));
            itable.setPk(rs.getString(3));
            itable.setBeginTime(rs.getString(4));
            itable.setEndTime(rs.getString(5));
        }
        rs.close();
        st.close();
        return itable;
    }

    public void addUpdate(Connection carbonConn,IcrmtTable itable) throws SQLException {
        String sql = "insert into carbon_odi_stat values ('%s','%s','%s','%s','%s')";
        sql = String.format(sql,itable.getCarbonTable(),itable.getHiveTable(),itable.getPk(),itable.getBeginTime()
                ,itable.getEndTime());
        Statement st = carbonConn.createStatement();
        st.execute(sql);
        st.close();
    }
}
