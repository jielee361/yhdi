package com.yinhai.yhdi.increment.update;

import com.yinhai.yhdi.common.CommonConn;
import com.yinhai.yhdi.common.OdiPrp;
import com.yinhai.yhdi.common.TargetConn;
import com.yinhai.yhdi.increment.parser.GPSqlParer;
import com.yinhai.yhdi.increment.poto.SqlPoto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
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
        Connection gpconnection = TargetConn.getGpconnection(icrmtConf.getTargetUrl(), icrmtConf.getTargetUsername()
                ,icrmtConf.getTargetpassword());
        Statement st = gpconnection.createStatement();
        gpconnection.setAutoCommit(false);
        GPSqlParer gpSqlParer = new GPSqlParer();
        while (!stopFlag) {
            //获取数据
            List<SqlPoto> sqlPotos = readNext();
            if (sqlPotos == null) {
                //logger.debug("update: 本次无新数据，暂停N秒再读取。");
                Thread.sleep(icrmtConf.getPauseTime());
                continue;
        }
            File file = new File(OdiPrp.getProperty("data.path"), String.valueOf(System.currentTimeMillis()));
            FileWriter fw  = new FileWriter(file);
            for (SqlPoto sqlPoto : sqlPotos) {
                st.addBatch(gpSqlParer.file2GpSql(sqlPoto));
                fw.write(gpSqlParer.file2GpSql(sqlPoto) + ";\n");
                //sqlPoto.printcol();
                //logger.debug("add sql:" + gpSqlParer.file2GpSql(sqlPoto));
            }
            fw.close();
            logger.debug("update: SQL准备完成-" + sqlPotos.size());
            st.executeBatch();
            gpconnection.commit();
            logger.debug("update: SQL执行完成-" + sqlPotos.size());
            //处理成功后弹出
            pollNext();
        }

        CommonConn.closeSt(st);
        CommonConn.closeConnection(gpconnection);

    }

}
