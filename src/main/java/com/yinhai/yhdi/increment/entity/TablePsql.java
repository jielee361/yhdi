package com.yinhai.yhdi.increment.entity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class TablePsql {
    private String tableName;
    private String isql;
    private String dsql;
    private Map<String,ArrayList<String>> idata;
    private Map<String,ArrayList<String>> ddata;

    public TablePsql(String tableName) {
        this.tableName = tableName;
        this.idata = new HashMap<>();
        this.ddata = new HashMap<>();
    }

    public String getIsql() {
        return isql;
    }

    public void setIsql(String isql) {
        this.isql = isql;
    }

    public String getDsql() {
        return dsql;
    }

    public void setDsql(String dsql) {
        this.dsql = dsql;
    }

    public Map<String, ArrayList<String>> getIdata() {
        return idata;
    }

    public Map<String, ArrayList<String>> getDdata() {
        return ddata;
    }

    public String getTableName() {
        return tableName;
    }
}
