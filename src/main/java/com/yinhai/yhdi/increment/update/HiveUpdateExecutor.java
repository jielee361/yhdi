package com.yinhai.yhdi.increment.update;

import com.yinhai.yhdi.common.CommonConn;
import com.yinhai.yhdi.increment.parser.HiveParser;
import com.yinhai.yhdi.increment.poto.SqlPoto;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

class HiveUpdateExecutor extends UpdateExecutor {
    private ArrayList<SqlPoto> sqlPotos;
    private Map<String, ArrayList<String>> tableRrecords;

    @Override
    public void startUpdate() throws Exception {
        //get meta
        Map<String, ArrayList<String>> tableMeta = gettableMeta();
        //parser
        HiveParser hiveParser = new HiveParser(tableMeta);
        while (!stopFlag) {
            //read next file
            sqlPotos = readNext();
            if (sqlPotos == null) {
                //logger.debug("update: 本次无新数据，暂停N秒再读取。");
                Thread.sleep(icrmtConf.getPauseTime());
                continue;
            }

            tableRrecords = hiveParser.file2HiveFile(sqlPotos);

            //loop write HDFS file
            for (Map.Entry<String,ArrayList<String>> map : tableRrecords.entrySet()) {
                //file name
                //open file
                //for ()

            }


        }



    }

    private Map<String,ArrayList<String>> gettableMeta() throws SQLException {
        //get jdbc
        Connection hiveConnection = CommonConn.getHiveConnection(icrmtConf.getTargetUrl(), icrmtConf.getTargetUsername()
                ,icrmtConf.getTargetpassword());
        //get col meta
        Map<String,ArrayList<String>> tableCols = new HashMap<>();
        String ttable="";
        String[] stables = icrmtConf.getSourceTable().split(",");
        Statement st = hiveConnection.createStatement();
        ResultSet rs = null;
        String colName;

        for (int i=0;i<stables.length;i++) {
            ttable =  stables[i].split("[.]")[1].replace("'","");
            rs = st.executeQuery("desc " + ttable);
            while (rs.next()) {
                colName = rs.getString(1).toUpperCase();
                if (tableCols.containsKey(ttable)) {
                    tableCols.get(ttable).add(colName);
                }else {
                    ArrayList<String> cols = new ArrayList<>();
                    cols.add(colName);
                    tableCols.put(ttable,cols);
                }
            }
        }
        CommonConn.closeRS(rs);
        CommonConn.closeSt(st);
        CommonConn.closeConnection(hiveConnection);
        return tableCols;

    }
}
