package com.yinhai.yhdi.batch.extract;

import com.yinhai.yhdi.batch.BatchDiConst;
import com.yinhai.yhdi.batch.entity.TaskStat;
import com.yinhai.yhdi.batch.entity.BatchTable;
import com.yinhai.yhdi.common.CommonConn;
import com.yinhai.yhdi.common.DiPrp;
import com.yinhai.yhdi.common.DiUtil;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;

public class OracleExtractExecutor extends ExtractExecutor{
    private BatchTable batchTable;
    private String taskName;
    private TaskStat taskStat;
    public OracleExtractExecutor(BatchTable batchTable,String taskName) {
        this.batchTable=batchTable;
        this.taskName = taskName;
        this.taskStat = batchTable.getTaskStatMap().get(taskName);
    }

    public void extractData() throws Exception {
        //拼接抽取SQL
        String partSql = taskStat.getPartSql();
        String extractSql ;
        if (DiUtil.isEmpty(partSql)) {
            extractSql = String.format(BatchDiConst.EXTRACT_SQL_ORA1,batchTable.getStable());
        }else {
            extractSql = String.format(BatchDiConst.EXTRACT_SQL_ORA2,batchTable.getStable(),taskStat.getPartSql());
        }
        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;
        try {
            //获取连接,执行查询
            conn = CommonConn.getOraConnection(batchTable.getSdb().getJdbcUrl()
                    , batchTable.getSdb().getUsername(), batchTable.getSdb().getPassword());
            st = conn.createStatement();
            st.setFetchSize(DiPrp.getIntProperty("extract.fetchsize"));
            //System.out.println("esql: "+esql);
            rs = st.executeQuery(extractSql);
            //写入文件
            writeData(rs);
            CommonConn.closeRS(rs);
            CommonConn.closeSt(st);
            CommonConn.closeConnection(conn);
        }catch (Exception e) {
            CommonConn.closeRS(rs);
            CommonConn.closeSt(st);
            CommonConn.closeConnection(conn);
            throw  new RuntimeException(e);
        }
    }
    void writeData(ResultSet rs) throws SQLException, IOException {
        //连接文件
        String fileName = batchTable.getDatapath()+ File.separator + taskName +".txt";
        File file = new File(fileName);
        FileWriter fw = new FileWriter(file);
        //
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        int rows = 0;
        StringBuilder sb = new StringBuilder();
        String cols;
        String field = DiPrp.getProperty("record.field");
        String record = DiPrp.getProperty("record.record");
        int printSize = DiPrp.getIntProperty("record.print.size");
        while (rs.next()) {
            sb.delete(0,sb.length());
            for (int i=1;i<=columnCount;i++) {
                cols = rs.getString(i);
                if (cols != null) {
                    sb.append(cols.replaceAll("\n|\r|\t",""));
                }
                sb.append(field);
            }
            sb.append(record);
            fw.write(sb.toString());
            //System.out.println(sb.toString());
            rows++;
            if (rows == printSize) {
                this.taskStat.addRows(rows);
                rows = 0;
            }
        }
        this.taskStat.addRows(rows);//最后批计入
        fw.flush();
        fw.close();

    }
}
