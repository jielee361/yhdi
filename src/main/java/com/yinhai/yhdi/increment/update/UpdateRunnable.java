package com.yinhai.yhdi.increment.update;

import com.yinhai.yhdi.batch.BatchDiConst;
import com.yinhai.yhdi.increment.IcrmtEnv;
import com.yinhai.yhdi.increment.entity.ThreadStat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateRunnable implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(UpdateRunnable.class);
    private String taskName;
    public UpdateRunnable(String taskName) {
        this.taskName =taskName;
    }
    @Override
    public void run() {
        ThreadStat threadStat = new ThreadStat();
        threadStat.setTname(taskName);
        threadStat.setBtime(System.currentTimeMillis());
        threadStat.setStat(BatchDiConst.RUN_STAT_RUNNING);
        IcrmtEnv.getThreadMap().put(taskName,threadStat);
        UpdateExecutor updateExecutor = new GPUpdateExecutor();
        try {
            logger.info("开始启动update线程！");
            updateExecutor.startUpdate();
            threadStat.setStat(BatchDiConst.RUN_STAT_SUCCESS);
            logger.info("update线程执行结束！");
        } catch (Exception e) {
            e.printStackTrace();
            threadStat.setStat(BatchDiConst.RUN_STAT_FAIL);
            threadStat.setTlog(e.getMessage());
            logger.error("update线程执行出错!");
        }

    }
}
