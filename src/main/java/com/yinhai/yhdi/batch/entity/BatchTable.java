package com.yinhai.yhdi.batch.entity;

import com.yinhai.yhdi.common.entity.DbConnInfo;

import java.util.concurrent.ConcurrentHashMap;

public class BatchTable {
    private String stable;
    private String ttable;
    private int extractParallel;
    private String datapath;
    private DbConnInfo sdb;
    private DbConnInfo tdb;
    private ConcurrentHashMap<String,TaskStat> taskStatMap;

    public BatchTable() {
        taskStatMap = new ConcurrentHashMap();
    }

    public ConcurrentHashMap<String, TaskStat> getTaskStatMap() {
        return taskStatMap;
    }

    public String getStable() {
        return stable;
    }

    public void setStable(String stable) {
        this.stable = stable;
    }

    public String getTtable() {
        return ttable;
    }

    public void setTtable(String ttable) {
        this.ttable = ttable;
    }

    public int getExtractParallel() {
        return extractParallel;
    }

    public void setExtractParallel(int extractParallel) {
        this.extractParallel = extractParallel;
    }

    public String getDatapath() {
        return datapath;
    }

    public void setDatapath(String datapath) {
        this.datapath = datapath;
    }

    public DbConnInfo getSdb() {
        return sdb;
    }

    public void setSdb(DbConnInfo sdb) {
        this.sdb = sdb;
    }

    public DbConnInfo getTdb() {
        return tdb;
    }

    public void setTdb(DbConnInfo tdb) {
        this.tdb = tdb;
    }

    @Override
    public String toString() {
        return "BatchTable{" +
                "stable='" + stable + '\'' +
                ", ttable='" + ttable + '\'' +
                ", extractParallel=" + extractParallel +
                ", datapath='" + datapath + '\'' +
                ", sdb=" + sdb.toString() +
                ", tdb=" + tdb.toString() +
                '}';
    }
}
