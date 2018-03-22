package com.yinhai.yhdi.increment.write;

import com.yinhai.yhdi.increment.entity.RedoObj;

import java.util.concurrent.LinkedBlockingDeque;

public abstract class WriteExecutor {
    public abstract void startWrite(LinkedBlockingDeque<RedoObj> redoQueue) throws Exception;
    public abstract void stopWrite();
}
