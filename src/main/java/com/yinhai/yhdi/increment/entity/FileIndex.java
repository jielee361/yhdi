package com.yinhai.yhdi.increment.entity;

import java.io.Serializable;

public class FileIndex implements Serializable {
    private long scn;
    private String rsid;
    private int ssn;


    public long getScn() {
        return scn;
    }

    public void setScn(long scn) {
        this.scn = scn;
    }

    public String getRsid() {
        return rsid;
    }

    public void setRsid(String rsid) {
        this.rsid = rsid;
    }

    public int getSsn() {
        return ssn;
    }

    public void setSsn(int ssn) {
        this.ssn = ssn;
    }
}
