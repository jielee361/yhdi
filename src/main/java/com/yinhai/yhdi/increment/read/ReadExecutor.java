package com.yinhai.yhdi.increment.read;

import com.yinhai.yhdi.increment.entity.RedoObj;

import java.util.concurrent.LinkedBlockingDeque;

public abstract  class ReadExecutor {
    /**
     * start read increment data from source database.
     * @param redoQueue
     * @throws Exception
     */
    public abstract void startRead(LinkedBlockingDeque<RedoObj> redoQueue) throws Exception;
    public abstract void stopRead();

}
