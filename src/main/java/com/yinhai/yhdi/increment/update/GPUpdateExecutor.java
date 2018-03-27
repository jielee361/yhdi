package com.yinhai.yhdi.increment.update;

import com.yinhai.yhdi.increment.poto.SqlPoto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * greenplum 更新类
 */
public class GPUpdateExecutor extends UpdateExecutor {
    private final static Logger logger = LoggerFactory.getLogger(GPUpdateExecutor.class);

    @Override
    public void startUpdate() throws Exception {
        while (!stopFlag) {
            //获取数据
            List<SqlPoto> sqlPotos = readNext();
            if (sqlPotos == null) {
                logger.debug("update: 本次无新数据，暂停N秒再读取。");
                Thread.sleep(icrmtConf.getPauseTime());
                continue;
            }
            for (SqlPoto sqlPoto : sqlPotos) {
                sqlPoto.printcol();
            }
            System.out.println("update获取到：" + sqlPotos.size());
            //处理成功后弹出
            pollNext();
        }

    }

}
