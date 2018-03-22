package com.yinhai.yhdi.increment.entity;

public class ThreadStat {
    private String tname;
    private int stat;
    private long btime;
    private long etime;
    private String tlog;

    public long getBtime() {
        return btime;
    }

    public void setBtime(long btime) {
        this.btime = btime;
    }

    public long getEtime() {
        return etime;
    }

    public void setEtime(long etime) {
        this.etime = etime;
    }

    public String getTname() {
        return tname;
    }

    public void setTname(String tname) {
        this.tname = tname;
    }

    public int getStat() {
        return stat;
    }

    public void setStat(int stat) {
        this.stat = stat;
        this.etime = System.currentTimeMillis();
    }

    public String getTlog() {
        return tlog;
    }

    public void setTlog(String tlog) {
        this.tlog = tlog;
    }
}
