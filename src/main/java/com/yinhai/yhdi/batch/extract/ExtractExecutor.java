package com.yinhai.yhdi.batch.extract;

import java.sql.ResultSet;

public abstract class ExtractExecutor {
    /**
     * 抽取数据
     */
    public abstract void extractData() throws Exception;

    /**
     * 写数据
     * @throws Exception
     */
    abstract void writeData(ResultSet rs) throws Exception;
}
