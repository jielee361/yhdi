package com.yinhai.yhdi.increment.write;

import com.yinhai.yhdi.increment.poto.RedoObj;
import com.yinhai.yhdi.increment.read.OraMetaOper;

import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

public abstract class WriteExecutor {
    boolean stopFlag;
    Map<String,String> pkMap;
    void initPkMap() throws Exception {
        this.pkMap = OraMetaOper.getTablePk();
    }
    public abstract void startWrite(LinkedBlockingDeque<RedoObj> redoQueue) throws Exception;
    public void stopWrite() {
        this.stopFlag = true;
    }
}
