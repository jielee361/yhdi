package com.yinhai.yhdi.increment.write;

import com.yinhai.yhdi.increment.poto.RedoObj;

import java.util.concurrent.LinkedBlockingDeque;

public class KafkaWriteExecutor extends WriteExecutor {
    @Override
    public void startWrite(LinkedBlockingDeque<RedoObj> redoQueue) throws Exception {


    }

    @Override
    public void stopWrite() {

    }
}
