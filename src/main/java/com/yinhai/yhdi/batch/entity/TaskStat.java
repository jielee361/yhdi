package com.yinhai.yhdi.batch.entity;

public class TaskStat {
    private int stat;
    private long utime;
    private long btime;
    private long rows;
    private String partSql;
    private String errLog;
    public TaskStat() {
        stat = 0;
        utime = 0l;
        btime = 0l;
        rows = 0l;
    }

    public void addRows(int addNum) {
        this.rows = this.rows + addNum;
    }

    public long getBtime() {
        return btime;
    }

    public void setBtime(long btime) {
        this.btime = btime;
    }

    public String getErrLog() {
        return errLog;
    }

    public void setErrLog(String errLog) {
        this.errLog = errLog;
    }

    public String getPartSql() {
        return partSql;
    }

    public void setPartSql(String partSql) {
        this.partSql = partSql;
    }

    public int getStat() {
        return stat;
    }

    public void setStat(int stat) {
        this.stat = stat;
        this.utime = System.currentTimeMillis();
    }

    public long getUtime() {
        return utime;
    }

    public void setUtime(long utime) {
        this.utime = utime;
    }

    public long getRows() {
        return rows;
    }

    public void setRows(long rows) {
        this.rows = rows;
    }

    @Override
    public String toString() {
        return "BatchRunstat{" +
                "stat=" + stat +
                ", utime=" + utime +
                ", btime=" + btime +
                ", rows=" + rows +
                ", partSql='" + partSql + '\'' +
                ", errLog='" + errLog + '\'' +
                '}';
    }
}
