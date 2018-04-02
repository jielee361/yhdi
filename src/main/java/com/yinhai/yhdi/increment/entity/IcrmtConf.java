package com.yinhai.yhdi.increment.entity;

public class IcrmtConf {
    //util
    private int maxTheadPoolSize;
    //read
    private String sourceDbkind;
    private String sourceUsername;
    private String sourcepassword;
    private String sourceUrl;
    private String sourceTable;//table's name which will use to sync,format: user1.table1,user2.table2
    private int redoQueueSize;
    private String lgmnrOpertion;
    private long lgmnrBeginScn;
    private String lgmnrSqlkind;
    private boolean isOracle12c;
    private String pdbUrl;

    //write
    private int fileSize;
    private int pauseTime;

    //update
    private String targetTable;
    private String targetDbkind;
    private String targetUrl;
    private String targetUsername;
    private String targetpassword;

    public String getPdbUrl() {
        return pdbUrl;
    }

    public void setPdbUrl(String pdbUrl) {
        this.pdbUrl = pdbUrl;
    }

    public String getTargetUrl() {
        return targetUrl;
    }

    public void setTargetUrl(String targetUrl) {
        this.targetUrl = targetUrl;
    }

    public String getTargetUsername() {
        return targetUsername;
    }

    public void setTargetUsername(String targetUsername) {
        this.targetUsername = targetUsername;
    }

    public String getTargetpassword() {
        return targetpassword;
    }

    public void setTargetpassword(String targetpassword) {
        this.targetpassword = targetpassword;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    public int getPauseTime() {
        return pauseTime;
    }

    public void setPauseTime(int pauseTime) {
        this.pauseTime = pauseTime;
    }

    public int getFileSize() {
        return fileSize;
    }

    public void setFileSize(int fileSize) {
        this.fileSize = fileSize;
    }

    public String getLgmnrSqlkind() {
        return lgmnrSqlkind;
    }

    public void setLgmnrSqlkind(String lgmnrSqlkind) {
        this.lgmnrSqlkind = lgmnrSqlkind;
    }


    public int getMaxTheadPoolSize() {
        return maxTheadPoolSize;
    }

    public void setMaxTheadPoolSize(int maxTheadPoolSize) {
        this.maxTheadPoolSize = maxTheadPoolSize;
    }

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
