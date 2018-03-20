package com.yinhai.yhdi.batch;

import com.yinhai.yhdi.batch.entity.BatchTable;
import com.yinhai.yhdi.batch.entity.TaskStat;
import com.yinhai.yhdi.batch.extract.ExtractRunnable;
import com.yinhai.yhdi.common.*;
import com.yinhai.yhdi.common.entity.DbConnInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class BatchDiDriver  {
    private BatchTable[] batchTables;
    private DbConnInfo sdb ;
    private DbConnInfo tdb ;
    Set<String> endTasks = new HashSet<String>();
    private static final Logger logger = LoggerFactory.getLogger(BatchDiDriver.class);

    public BatchDiDriver(String stable, String dataPath, int parallel) {
        initPrp();
        batchTables = new BatchTable[1];
        BatchTable batchTable = new BatchTable();
        batchTable.setStable(stable);
        batchTable.setDatapath(dataPath + File.separator + stable);
        batchTable.setExtractParallel(parallel);
        batchTable.setSdb(this.sdb);
        batchTable.setTdb(this.tdb);
    }

    public BatchDiDriver() {
        initPrp();
        String prpTables = OdiPrp.getProperty("tables");
        String[] tableArry = prpTables.split("[|]");
        int lln = tableArry.length;
        if (OdiUtil.isEmpty(tableArry[lln-1])) {
            lln = lln - 1;
        }
        if (lln == 0) {
            throw new RuntimeException("未正确配置同步表名!");
        }
        //初始化要抽取的表信息到数组batchTables
        batchTables = new BatchTable[lln];
        for (int i=0;i<lln;i++) {
            String[] tableInfo = tableArry[i].split("-");
            BatchTable batchTable = new BatchTable();
            batchTable.setStable(tableInfo[0]);
            if (tableInfo.length==2) {
                batchTable.setTtable(tableInfo[1]);
            }
            batchTable.setDatapath(OdiPrp.getProperty("datafile.patch") + File.separator + tableInfo[0]);
            batchTable.setExtractParallel(OdiPrp.getIntProperty("extract.parallel"));
            batchTable.setSdb(this.sdb);
            batchTable.setTdb(this.tdb);
            batchTables[i] = batchTable;
        }

    }
    private void initPrp() {
        sdb = new DbConnInfo(OdiPrp.getProperty("sdb.url"),OdiPrp.getProperty("sdb.username")
                ,OdiPrp.getProperty("sdb.password"),OdiPrp.getProperty("sdb.kind"));
        tdb = new DbConnInfo(OdiPrp.getProperty("sdb.url"),"","",
                OdiPrp.getProperty("tdb.kind"));

    }
    public void startExtract() throws Exception {
        if (this.batchTables == null) {
            throw new RuntimeException("抽取的表信息batchTables未初始化成功，请检查配置文件！");
        }
        Connection conn = CommonConn.getOraConnection(sdb.getJdbcUrl(), sdb.getUsername(), sdb.getPassword());
        Statement st = conn.createStatement();
        ThreadPoolUtil threadPool = ThreadPoolUtil.getThreadPool(OdiPrp.getIntProperty("thread.num"));
        //开始循环提交所有表
        for (int i=0;i<batchTables.length;i++) {
            BatchTable bt = batchTables[i];
            if (bt.getExtractParallel()>1) {//多线程
                //计算分片
                String[] stableInfo = bt.getStable().split("[.]");
                String getPartSql = String.format(BatchDiConst.GET_PART_SQL,bt.getExtractParallel()
                        ,stableInfo[1].toUpperCase(),stableInfo[0].toUpperCase());
                //System.out.println("partSql: "+getPartSql);
                ResultSet rs = st.executeQuery(getPartSql);
                int threadId=1;
                while (rs.next()) {
                    TaskStat taskStat = new TaskStat();
                    taskStat.setPartSql("'" + rs.getString(1).replace("field","' and '") + "'");
                    bt.getTaskStatMap().put(bt.getStable()+"-task"+threadId,taskStat);
                    threadId++;

                }
                CommonConn.closeRS(rs);
            }else {//单线程抽取
                TaskStat taskStat = new TaskStat();
                bt.getTaskStatMap().put(bt.getStable()+"-task1",taskStat);
            }
            //创建目录
            File file = new File(bt.getDatapath());
            if (file.exists() && file.isDirectory()) {
                String[] children = file.list();
                for (int m=0; m<children.length; m++) {
                    String fileName = bt.getDatapath() + File.separator + children[m];
                    new File(fileName).delete();
                }
            }else {
                file.mkdir();
            }

            //提交线程
            for (Map.Entry<String,TaskStat> taskStat:bt.getTaskStatMap().entrySet()) {
                ExtractRunnable er = ExtractRunnable.getExtractRunable(bt,taskStat.getKey());
                taskStat.getValue().setStat(BatchDiConst.RUN_STAT_QUEUE);
                threadPool.submit(taskStat.getKey(),er);
            }
            //如果表之间不并行抽取，阻塞下个表的提交
            if (!OdiPrp.getProperty("table.isparallel").equals("true")) {
                monitorThread();
            }
        }
        //提交完后关闭ORACLE连接
        CommonConn.closeSt(st);
        CommonConn.closeConnection(conn);
        //开始监控各个线程抽取情况
        if (OdiPrp.getProperty("table.isparallel").equals("true")) {
            monitorThread();
        }
        //所有抽取完成，关闭线程池
        threadPool.close();

    }

    private void monitorThread() {
        int runningNum = 0;
        while (true) {
            runningNum = 0;
            //遍历每张表
            for (int i=0;i<batchTables.length;i++) {
                //遍历每个表的每个线程
                for(Map.Entry<String,TaskStat> taskStatEntry:batchTables[i].getTaskStatMap().entrySet()) {
                    TaskStat taskStat = taskStatEntry.getValue();
                    String taskName = taskStatEntry.getKey();
                    if (endTasks.contains(taskName)) continue;
                    int runStat = taskStat.getStat();
                    //System.out.println(br.getKey()+" runSta : " + runStat);
                    long runtime ;
                    if (taskStat.getBtime() == 0) {
                        runtime = 0;
                    }else {
                        runtime = (System.currentTimeMillis() - taskStat.getBtime()) / 1000;
                    }
                    switch (runStat) {
                        case BatchDiConst.RUN_STAT_RUNNING :
                            printRunInfo(taskName,"正在运行",taskStat.getRows(),runtime);
                            runningNum++;
                            break;
                        case BatchDiConst.RUN_STAT_SUCCESS : if (endTasks.contains(taskName)) break;
                            printRunInfo(taskName,"运行成功",taskStat.getRows(),
                                    (taskStat.getUtime() - taskStat.getBtime()) / 1000 );
                            endTasks.add(taskName);
                            break;
                        case BatchDiConst.RUN_STAT_FAIL : if (endTasks.contains(taskName)) break;
                            printRunInfo(taskName,"运行失败\n" + taskStat.getErrLog(),taskStat.getRows(),runtime);
                            endTasks.add(taskName);
                            break;
                        case BatchDiConst.RUN_STAT_QUEUE :
                            printRunInfo(taskName,"等待执行",taskStat.getRows(),runtime);
                            runningNum++;
                            break;
                        case 0 : break;//初始状态，还未提交，不做处理
                        default:logger.info("不识别的运行状态："+runStat);
                    }

                }

            }
            logger.info("== running task num: " + runningNum);
            if (runningNum == 0) {//已没有还在运行的程序。
                logger.info("==此表监控完成！");
                break;
            }

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

    private void printRunInfo(String taskName,String taskStat,long rows,long runTime) {
        logger.info(String.format(BatchDiConst.EXTRACT_STAT_OUTP,taskName,taskStat,rows,runTime));

    }
}
