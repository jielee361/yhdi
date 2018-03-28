package com.yinhai.yhdi.increment.update;

import com.yinhai.yhdi.common.CommonConn;
import com.yinhai.yhdi.common.TargetConn;
import com.yinhai.yhdi.increment.parser.GPSqlParer;
import com.yinhai.yhdi.increment.poto.SqlPoto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

/**
 * greenplum 更新类
 */
public class GPUpdateExecutor extends UpdateExecutor {
    private final static Logger logger = LoggerFactory.getLogger(GPUpdateExecutor.class);

    @Override
    public void startUpdate() throws Exception {
        //get jdbc
        Connection gpconnection = TargetConn.getGpconnection("jdbc:postgresql://192.168.26.220:25432/template1",
                "greenplum", "");
        Statement st = gpconnection.createStatement();
        GPSqlParer gpSqlParer = new GPSqlParer();
        while (!stopFlag) {
            //获取数据
            List<SqlPoto> sqlPotos = readNext();
            if (sqlPotos == null) {
                logger.debug("update: 本次无新数据，暂停N秒再读取。");
                Thread.sleep(icrmtConf.getPauseTime());
                continue;
            }
            for (SqlPoto sqlPoto : sqlPotos) {
                st.addBatch(gpSqlParer.file2GpSql(sqlPoto));
                sqlPoto.printcol();
                System.out.println(gpSqlParer.file2GpSql(sqlPoto));
            }
            System.out.println("update获取到：" + sqlPotos.size());
            st.executeBatch();
            //处理成功后弹出
            pollNext();
        }

        //CommonConn.closeSt(st);
        //CommonConn.closeConnection(gpconnection);

    }

}
