package com.yinhai.yhdi.increment.write;

import com.yinhai.yhdi.batch.BatchDiConst;
import com.yinhai.yhdi.common.CommonConn;
import com.yinhai.yhdi.common.DiPrp;
import com.yinhai.yhdi.increment.IcrmtEnv;
import com.yinhai.yhdi.increment.entity.IcrmtConf;
import com.yinhai.yhdi.increment.entity.ThreadStat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteRunnable implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(WriteRunnable.class);
    private String taskName;
    public WriteRunnable(String taskName) {
        this.taskName = taskName;
    }
    @Override
    public void run() {
        IcrmtConf icrmtConf = IcrmtEnv.getIcrmtConf();
        ThreadStat threadStat = new ThreadStat();
        threadStat.setTname(taskName);
        threadStat.setBtime(System.currentTimeMillis());
        threadStat.setStat(BatchDiConst.RUN_STAT_RUNNING);
        IcrmtEnv.getThreadMap().put(taskName,threadStat);
        WriteExecutor writeExecutor;
        if ("true".equals(DiPrp.getProperty("kafka.isopen"))) {
            writeExecutor = new KafkaWriteExecutor();
        }else {
            writeExecutor = new FileWriteExecutor();
        }
        try {
            logger.info("开始启动write线程！");
            //IcrmtEnv.getRedoQueue().clear();
            writeExecutor.startWrite(IcrmtEnv.getRedoQueue());
            threadStat.setStat(BatchDiConst.RUN_STAT_SUCCESS);
            logger.info("write线程执行结束！");
        } catch (Exception e) {
            e.printStackTrace();
            threadStat.setStat(BatchDiConst.RUN_STAT_FAIL);
            threadStat.setTlog(e.getMessage());
            logger.error("write线程执行出错!");
        }

    }
}
