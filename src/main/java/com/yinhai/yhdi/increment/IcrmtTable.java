package com.yinhai.yhdi.increment;

public class IcrmtTable {
    private String hiveTable;
    private String carbonTable;
    private String pk;
    private String beginTime;
    private String endTime;

    public String getHiveTable() {
        return hiveTable;
    }

    public void setHiveTable(String hiveTable) {
        this.hiveTable = hiveTable;
    }

    public void setCarbonTable(String carbonTable) {
        this.carbonTable = carbonTable;
    }

    public String getCarbonTable() {
        return carbonTable;
    }

    public String getPk() {
        return pk;
    }
    public void setPk(String pk) {
        this.pk = pk;
    }

    public String getBeginTime() {
        return beginTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setBeginTime(String beginTime) {
        this.beginTime = beginTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public String toString() {
        return "carbonTable:"+carbonTable+" pk:"+pk+" beginTime:"+beginTime+" endTime"+endTime
                +"hiveTable:"+hiveTable;
    }
}
