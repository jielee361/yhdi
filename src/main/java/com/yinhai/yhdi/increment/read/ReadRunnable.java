package com.yinhai.yhdi.increment.read;

import com.yinhai.yhdi.batch.BatchDiConst;
import com.yinhai.yhdi.common.CommonConn;
import com.yinhai.yhdi.increment.IcrmtEnv;
import com.yinhai.yhdi.increment.entity.IcrmtConf;
import com.yinhai.yhdi.increment.entity.ThreadStat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

public class ReadRunnable implements Runnable{
    private final static Logger logger = LoggerFactory.getLogger(ReadRunnable.class);
    private  String taskName;
    public ReadRunnable(String taskName) {
        this.taskName = taskName;
    }
    @Override
    public void run() {
        logger.info("开始启动read线程！");
        IcrmtConf icrmtConf = IcrmtEnv.getIcrmtConf();
        //get the jdbc connection
        Connection conn;
        try {
            conn = CommonConn.getOraConnection(icrmtConf.getSourceUrl(),
                    icrmtConf.getSourceUsername(), icrmtConf.getSourcepassword());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("获取JDBC连接出错！");
        }
        ReadExecutor readExecutor = new OraReadExecutor(conn);
        ThreadStat threadStat = new ThreadStat();
        threadStat.setTname(taskName);
        threadStat.setBtime(System.currentTimeMillis());
        threadStat.setStat(BatchDiConst.RUN_STAT_RUNNING);
        IcrmtEnv.getThreadMap().put(taskName,threadStat);
        try {
            IcrmtEnv.getRedoQueue().clear();
            readExecutor.startRead(IcrmtEnv.getRedoQueue());
            threadStat.setStat(BatchDiConst.RUN_STAT_SUCCESS);
            CommonConn.closeConnection(conn);
            logger.info("read线程执行结束！");
        } catch (Exception e) {
            e.printStackTrace();
            threadStat.setStat(BatchDiConst.RUN_STAT_FAIL);
            threadStat.setTlog(e.getMessage());
            logger.error("read线程执行出错!");
            CommonConn.closeConnection(conn);
        }

    }
}
