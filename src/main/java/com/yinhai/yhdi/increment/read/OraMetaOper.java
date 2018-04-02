package com.yinhai.yhdi.increment.read;

import com.yinhai.yhdi.common.CommonConn;
import com.yinhai.yhdi.increment.IcrmtCost;
import com.yinhai.yhdi.increment.IcrmtEnv;
import com.yinhai.yhdi.increment.entity.IcrmtConf;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class OraMetaOper {
    public static Map<String,String> getTablePk() throws Exception {
        IcrmtConf icrmtConf = IcrmtEnv.getIcrmtConf();
        String url;
        if (icrmtConf.isOracle12c()) {
            url = icrmtConf.getPdbUrl();
        }else {
            url = icrmtConf.getSourceUrl();
        }
        Connection conn = CommonConn.getOraConnection(url,
                    icrmtConf.getSourceUsername(), icrmtConf.getSourcepassword());
        String getPkSql = String.format(IcrmtCost.ORA_PK_SQL,icrmtConf.getSourceTable().toUpperCase());
        Map<String,String> pkMap = new HashMap<>();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(getPkSql);
        while (rs.next()) {
            String table = rs.getString(1) + "." + rs.getString(3);
            if (pkMap.containsKey(table)) {
                pkMap.put(table,pkMap.get(table) + "," + rs.getString(4));
            }else {
                pkMap.put(table,rs.getString(4));
            }
        }
        CommonConn.closeConnection(conn);
        return pkMap;



    }
}
