package com.yinhai.yhdi.increment;

import com.yinhai.yhdi.common.ThreadPoolUtil;
import com.yinhai.yhdi.increment.entity.FileIndex;
import com.yinhai.yhdi.increment.entity.IcrmtConf;
import com.yinhai.yhdi.increment.poto.IndexQueue;
import com.yinhai.yhdi.increment.poto.RedoObj;
import com.yinhai.yhdi.increment.entity.ThreadStat;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

public class IcrmtEnv {
    private static ConcurrentHashMap<String,ThreadStat> threadMap; //线程状态监控
    private static ThreadPoolUtil threadPool;//线程池
    private static IcrmtConf icrmtConf;//配置
    private static final LinkedBlockingDeque<RedoObj> redoQueue = new LinkedBlockingDeque<>();//数据缓存队列
    private static IndexQueue indexQueue;//索引缓存队列
    private static FileIndex lastIndex;//上次抽取节点

    public static FileIndex getLastIndex() {
        return lastIndex;
    }

    public static void setLastIndex(FileIndex lastIndex) {
        IcrmtEnv.lastIndex = lastIndex;
    }

    public static IndexQueue getIndexQueue() {
        return indexQueue;
    }

    public static void setIndexQueue(IndexQueue indexQueue) {
        IcrmtEnv.indexQueue = indexQueue;
    }

    public static void init(ConcurrentHashMap<String,ThreadStat> threadMap,
                            ThreadPoolUtil threadPool,
                            IcrmtConf icrmtConf) {
        IcrmtEnv.threadMap = threadMap;
        IcrmtEnv.threadPool = threadPool;
        IcrmtEnv.icrmtConf = icrmtConf;
    }

    public static ConcurrentHashMap<String,ThreadStat> getThreadMap() {
        return threadMap;
    }

    public static ThreadPoolUtil getThreadPool() {
        return threadPool;
    }

    public void init(int maxThreadPool) {
        threadMap = new ConcurrentHashMap<>();
        threadPool = ThreadPoolUtil.getThreadPool(maxThreadPool);

    }

    public static IcrmtConf getIcrmtConf() {
        return icrmtConf;
    }

    public static void setIcrmtConf(IcrmtConf icrmtConf) {
        IcrmtEnv.icrmtConf = icrmtConf;
    }

    public static LinkedBlockingDeque<RedoObj> getRedoQueue() {
        return redoQueue;
    }
}
