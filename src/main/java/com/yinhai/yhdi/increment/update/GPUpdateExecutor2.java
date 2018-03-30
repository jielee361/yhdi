package com.yinhai.yhdi.increment.update;

import com.yinhai.yhdi.common.CommonConn;
import com.yinhai.yhdi.increment.IcrmtCost;
import com.yinhai.yhdi.increment.entity.TablePsql;
import com.yinhai.yhdi.increment.parser.GPSqlParser2;
import com.yinhai.yhdi.increment.poto.SqlPoto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GPUpdateExecutor2 extends UpdateExecutor {
    private final static Logger logger = LoggerFactory.getLogger(GPUpdateExecutor2.class);
    @Override
    public void startUpdate() throws Exception {
        //get jdbc
        Connection gpconnection = CommonConn.getGpconnection(icrmtConf.getTargetUrl(), icrmtConf.getTargetUsername()
                ,icrmtConf.getTargetpassword());
        //get col meta
        Map<String,ArrayList<String>> tableCols = new HashMap<>();
        String ttables="";
        String[] stables = icrmtConf.getSourceTable().split(",");
        for (int i=0;i<stables.length;i++) {
            ttables = ttables + ",'" + stables[i].split("[.]")[1];
        }
        Statement st = gpconnection.createStatement();
        ResultSet rs = st.executeQuery(String.format(IcrmtCost.GP_COL_SQL, ttables.substring(1).toLowerCase()));
        String tableName;
        String colName;
        while (rs.next()) {
            tableName = rs.getString(1).toUpperCase();
            colName = rs.getString(2).toUpperCase();
            if (tableCols.containsKey(tableName)) {
                tableCols.get(tableName).add(colName);
            }else {
                ArrayList<String> cols = new ArrayList<>();
                cols.add(colName);
                tableCols.put(tableName,cols);
            }
        }
        CommonConn.closeRS(rs);
        CommonConn.closeSt(st);

        //begin to update
        GPSqlParser2 gpSqlParser2 = new GPSqlParser2(tableCols);
        PreparedStatement pst;
        while (!stopFlag) {
            //get sqlPoto from file
            List<SqlPoto> sqlPotos = readNext();
            if (sqlPotos == null) {
                //logger.debug("update: 本次无新数据，暂停N秒再读取。");
                Thread.sleep(icrmtConf.getPauseTime());
                continue;
            }
            //parse sql
            Map<String, TablePsql> psqlMap = gpSqlParser2.file2Psql(sqlPotos);
            for (Map.Entry<String,TablePsql> map : psqlMap.entrySet()) { //表循环
                TablePsql psql = map.getValue();
                //先删除
                if (psql.getDdata().size() > 0) {
                    pst = gpconnection.prepareStatement(psql.getDsql());
                    for (Map.Entry<String,ArrayList<String>> dmap : psql.getDdata().entrySet()) {//行循环
                        for (int k=0;k<dmap.getValue().size();k++) {//字段循环
                            pst.setString(k + 1,dmap.getValue().get(k));
                        }
                        pst.addBatch();
                    }
                    logger.debug("开始执行删除：" + psql.getDdata().size() );
                    pst.executeBatch();
                    CommonConn.closePS(pst);
                    logger.debug("删除完成！");
                }
                //再插入
                if (psql.getIdata().size() > 0) {
                    pst = gpconnection.prepareStatement(psql.getIsql());
                    for (Map.Entry<String,ArrayList<String>> imap : psql.getIdata().entrySet()) {//行循环
                        for (int k=0;k<imap.getValue().size();k++) {//字段循环
                            pst.setString(k + 1,imap.getValue().get(k));
                        }
                        pst.addBatch();
                    }
                    logger.debug("开始执行插入：" + psql.getIdata().size() );
                    pst.executeBatch();
                    CommonConn.closePS(pst);
                    logger.debug("插入完成！");
                }

            }

            //这个文件执行成功，弹出队列，删除文件
            pollNext();
        }


    }
}
