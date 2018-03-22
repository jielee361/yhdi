package com.yinhai.yhdi.increment.entity;

public class IcrmtConf {
    private String sourceDbkind;
    private String sourceUsername;
    private String sourcepassword;
    private String sourceUrl;
    private String targetDbkind;
    private String tableString;//table's name which will use to sync,format: user1.table1,user2.table2

    private int maxTheadPoolSize;

    private String lgmnrOpertion;
    private long lgmnrBeginScn;
    private String lgmnrSqlkind;

    public String getLgmnrSqlkind() {
        return lgmnrSqlkind;
    }

    public void setLgmnrSqlkind(String lgmnrSqlkind) {
        this.lgmnrSqlkind = lgmnrSqlkind;
    }

    private long lastScn;
    private int lastSsn;
    private String lastRsid;
    private boolean isOracle12c;


    public int getMaxTheadPoolSize() {
        return maxTheadPoolSize;
    }

    public void setMaxTheadPoolSize(int maxTheadPoolSize) {
        this.maxTheadPoolSize = maxTheadPoolSize;
    }

    public long getLastScn() {
        return lastScn;
    }

    public void setLastScn(long lastScn) {
        this.lastScn = lastScn;
    }

    public int getLastSsn() {
        return lastSsn;
    }

    public void setLastSsn(int lastSsn) {
        this.lastSsn = lastSsn;
    }

    public String getLastRsid() {
        return lastRsid;
    }

    public void setLastRsid(String lastRsid) {
        this.lastRsid = lastRsid;
    }

    private int redoQueueSize;

    public int getRedoQueueSize() {
        return redoQueueSize;
    }

    public void setRedoQueueSize(int redoQueueSize) {
        this.redoQueueSize = redoQueueSize;
    }

    public boolean isOracle12c() {
        return isOracle12c;
    }

    public void setOracle12c(boolean oracle12c) {
        isOracle12c = oracle12c;
    }

    public String getTableString() {
        return tableString;
    }

    public void setTableString(String tableString) {
        this.tableString = tableString;
    }

    public String getSourceDbkind() {
        return sourceDbkind;
    }

    public void setSourceDbkind(String sourceDbkind) {
        this.sourceDbkind = sourceDbkind;
    }

    public String getSourceUsername() {
        return sourceUsername;
    }

    public void setSourceUsername(String sourceUsername) {
        this.sourceUsername = sourceUsername;
    }

    public String getSourcepassword() {
        return sourcepassword;
    }

    public void setSourcepassword(String sourcepassword) {
        this.sourcepassword = sourcepassword;
    }

    public String getSourceUrl() {
        return sourceUrl;
    }

    public void setSourceUrl(String sourceUrl) {
        this.sourceUrl = sourceUrl;
    }

    public String getTargetDbkind() {
        return targetDbkind;
    }

    public void setTargetDbkind(String targetDbkind) {
        this.targetDbkind = targetDbkind;
    }

    public String getLgmnrOpertion() {
        return lgmnrOpertion;
    }

    public void setLgmnrOpertion(String lgmnrOpertion) {
        this.lgmnrOpertion = lgmnrOpertion;
    }

    public long getLgmnrBeginScn() {
        return lgmnrBeginScn;
    }

    public void setLgmnrBeginScn(long lgmnrBeginScn) {
        this.lgmnrBeginScn = lgmnrBeginScn;
    }
}
