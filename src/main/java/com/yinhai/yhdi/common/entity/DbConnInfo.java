package com.yinhai.yhdi.common.entity;

public class DbConnInfo {
    public DbConnInfo(String jdbcUrl,String username,String password,String dbKind){
        this.setJdbcUrl(jdbcUrl);
        this.setUsername(username);
        this.setPassword(password);
        this.setDbKind(dbKind);
    }
    private String jdbcUrl;
    private String username;
    private String password;
    private String dbKind;

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDbKind() {
        return dbKind;
    }

    public void setDbKind(String dbKind) {
        this.dbKind = dbKind;
    }

    @Override
    public String toString() {
        return "DbConnInfo{" +
                "jdbcUrl='" + jdbcUrl + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", dbKind='" + dbKind + '\'' +
                '}';
    }
}
