package com.yinhai.yhdi.increment;

import com.yinhai.yhdi.common.ThreadPoolUtil;
import com.yinhai.yhdi.increment.entity.IcrmtConf;
import com.yinhai.yhdi.increment.entity.RedoObj;
import com.yinhai.yhdi.increment.entity.ThreadStat;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

public class IcrmtEnv {
    private static ConcurrentHashMap<String,ThreadStat> threadMap;
    private static ThreadPoolUtil threadPool;
    private static IcrmtConf icrmtConf;
    private static final LinkedBlockingDeque<RedoObj> redoQueue = new LinkedBlockingDeque<>();

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
