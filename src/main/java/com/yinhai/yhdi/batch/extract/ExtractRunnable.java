package com.yinhai.yhdi.batch.extract;

import com.yinhai.yhdi.batch.BatchDiConst;
import com.yinhai.yhdi.batch.entity.BatchTable;
import com.yinhai.yhdi.batch.entity.TaskStat;

public  class ExtractRunnable implements Runnable {
    private String taskName;
    private TaskStat taskStat;
    private ExtractExecutor extractExecutor;

    /**
     * 获取抽取的Runnable实例，并初始化batchTable、taskName、taskStat
     * @param batchTable
     * @param taskName
     * @return
     */
    public static ExtractRunnable getExtractRunable(BatchTable batchTable,String taskName) {
        return new ExtractRunnable( batchTable, taskName);
    }

    public ExtractRunnable(BatchTable batchTable,String taskName) {
        this.taskName = taskName;
        this.taskStat  = batchTable.getTaskStatMap().get(taskName);
        if (BatchDiConst.DB_KIND_ORACLE.equals(batchTable.getSdb().getDbKind())) {//如果是oracle源端
            extractExecutor = new OracleExtractExecutor(batchTable, taskName);
        }else {
            throw new RuntimeException("不识别的源数据库类型："+batchTable.getSdb().getDbKind());
        }
    }

    @Override
    public void run() {
        try {
            taskStat.setStat(BatchDiConst.RUN_STAT_RUNNING);
            taskStat.setBtime(System.currentTimeMillis());
            extractExecutor.extractData();
            taskStat.setStat(BatchDiConst.RUN_STAT_SUCCESS);

        }catch (Exception e) {
            e.printStackTrace();
            taskStat.setStat(BatchDiConst.RUN_STAT_FAIL);
            taskStat.setErrLog("线程："+taskName+" 执行出错！\n "+e.getMessage());

        }

    }

}
