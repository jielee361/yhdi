package com.yinhai.yhdi.increment;

import com.yinhai.yhdi.common.CommonConn;
import com.yinhai.yhdi.common.OdiPrp;

import java.sql.*;

public class IcrmtHiveToCarbon {
    /**
     * 从HIVE更新增量数据到CARBONDATA
     * @param icrmtTable 需要更新的表
     */
    public void updateDataToCarbon(IcrmtTable icrmtTable) throws SQLException {
        //获取carbon连接.
        String url = OdiPrp.getProperty("carbon.url");
        String username = OdiPrp.getProperty("carbon.username");
        String password = OdiPrp.getProperty("carbon.password");
        Connection carbonConnection = CommonConn.getHiveConnection(url, username, password);
        //加载增量数据到CARBON
        //判断有无增量数据，无则直接结束该表更新。
        //执行删除更新。
        deleteData(icrmtTable,carbonConnection);
        //执行加载更新。
        appendData(icrmtTable,carbonConnection);
        CommonConn.closeConnection(carbonConnection);
    }
    private void deleteData(IcrmtTable itable,Connection conn) throws SQLException {
        String deleteSql = OdiPrp.getProperty("sql.delete");
        String carbonTable=itable.getCarbonTable();
        String hvieTable = itable.getHiveTable();
        deleteSql = deleteSql.replace("carbon_table",carbonTable).replaceAll("col_pk",
                itable.getPk()).replace("hive_table",hvieTable).replace("b_time",
                itable.getBeginTime()).replace("e_time",itable.getEndTime());
        Statement statement = conn.createStatement();
        System.out.println(">> deleteSql:"+deleteSql);
        statement.execute(deleteSql);
        statement.close();
    }
    private void appendData(IcrmtTable itable,Connection conn) throws SQLException {
        //获取字段
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("select * from " + itable.getCarbonTable() + " limit 0");
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        StringBuilder sb = new StringBuilder();
        for (int i=1;i<=columnCount;i++) {
            sb.append(metaData.getColumnName(i));
            sb.append(",");
        }
        String cols = sb.toString().substring(0,sb.length()-1);
        rs.close();
        //拼接SQL
        String carbonTable=itable.getCarbonTable();
        String hvieTable = itable.getHiveTable();
        String appendSql = OdiPrp.getProperty("sql.append");
        String selectSql = OdiPrp.getProperty("sql.select");
        selectSql = selectSql.replace("col_pk",itable.getPk()).replace("hive_table",hvieTable)
                .replace("b_time",itable.getBeginTime()).replace("e_time",itable.getEndTime());
        appendSql = String.format(appendSql,carbonTable,cols,selectSql);
        System.out.println(">>insql:"+appendSql);
        //执行插入
        st.execute(appendSql);
        st.close();
    }

}
