package com.yinhai.yhdi.increment;

import com.yinhai.yhdi.common.OdiPrp;
import com.yinhai.yhdi.common.ThreadPoolUtil;
import com.yinhai.yhdi.increment.entity.FileIndex;
import com.yinhai.yhdi.increment.entity.IcrmtConf;
import com.yinhai.yhdi.increment.entity.ThreadStat;
import com.yinhai.yhdi.increment.poto.IndexQueue;
import com.yinhai.yhdi.increment.read.ReadRunnable;
import com.yinhai.yhdi.increment.update.UpdateRunnable;
import com.yinhai.yhdi.increment.write.WriteRunnable;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class SourceServer {
    private static final String readTaskName = "yhdi-read-task";
    private static final String writeTaskName = "yhdi-write-task";
    private static final String updateTaskName = "yhdi-update-task";
    public static void main(String[] args) {
        //create work dir
        createWorkDir();

        //init ENV
        IcrmtConf icrmtConf = getConfFromPrp();
        ThreadPoolUtil threadPool = ThreadPoolUtil.getThreadPool(icrmtConf.getMaxTheadPoolSize());
        ConcurrentHashMap<String,ThreadStat> threadMap = new ConcurrentHashMap<>();
        IcrmtEnv.init(threadMap,threadPool,icrmtConf);
        IndexQueue indexQueue = new IndexQueue();
        try {
            indexQueue.startup();//初始化索引队列
        } catch (IOException e) {
            e.printStackTrace();
        }
        IcrmtEnv.setIndexQueue(indexQueue);

        //start read
        ReadRunnable readRunnable = new ReadRunnable(readTaskName);
        try {
            IcrmtEnv.getThreadPool().submit(readTaskName,readRunnable);
        } catch (Exception e) {
            e.printStackTrace();
        }
        //start write
        WriteRunnable writeRunnable = new WriteRunnable(writeTaskName);
        try {
            IcrmtEnv.getThreadPool().submit(writeTaskName,writeRunnable);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //start update
        UpdateRunnable updateRunnable = new UpdateRunnable(updateTaskName);
        try {
            IcrmtEnv.getThreadPool().submit(updateTaskName,updateRunnable);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static IcrmtConf getConfFromPrp() {
        IcrmtConf icrmtConf = new IcrmtConf();
        icrmtConf.setLgmnrBeginScn(OdiPrp.getLongProperty("lgmnr.begin.scn"));
        icrmtConf.setLgmnrOpertion(OdiPrp.getProperty("lgmnr.opertion"));
        icrmtConf.setOracle12c((OdiPrp.getProperty("isoracle12c").equals("true")));
        icrmtConf.setRedoQueueSize(OdiPrp.getIntProperty("redoqueue.size"));
        icrmtConf.setSourceDbkind(OdiPrp.getProperty("source.dbkind"));
        icrmtConf.setSourcepassword(OdiPrp.getProperty("source.password"));
        icrmtConf.setSourceUrl(OdiPrp.getProperty("source.url"));
        icrmtConf.setSourceUsername(OdiPrp.getProperty("source.username"));
        icrmtConf.setSourceTable(OdiPrp.getProperty("source.table").toUpperCase());
        icrmtConf.setTargetDbkind(OdiPrp.getProperty("target.dbkind"));
        icrmtConf.setMaxTheadPoolSize(OdiPrp.getIntProperty("threadpool.maxsize"));
        icrmtConf.setLgmnrSqlkind(OdiPrp.getProperty("lgmnr.sqlkind"));
        icrmtConf.setFileSize(OdiPrp.getIntProperty("file.size"));
        icrmtConf.setPauseTime(OdiPrp.getIntProperty("pause.time"));
        return icrmtConf;
    }

    private static void createWorkDir() {
        File dataDir = new File(OdiPrp.getProperty("data.path"));
        if (!dataDir.exists()) {
            dataDir.mkdir();
        }
        File indexDir = new File(OdiPrp.getProperty("index.path"));
        if (!indexDir.exists()) {
            indexDir.mkdir();
        }
    }
}
