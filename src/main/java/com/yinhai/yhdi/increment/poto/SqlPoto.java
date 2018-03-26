package com.yinhai.yhdi.increment.poto;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class SqlPoto implements Serializable {
    private String table;
    private String user;
    private String opType;
    private String pk;
    private Map<String,String> after;

    public SqlPoto() {
        after = new HashMap<>();
    }

    public void putCol(String colName,String colValue) {
        this.after.put(colName,colValue);
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getOpType() {
        return opType;
    }

    public void setOpType(String opType) {
        this.opType = opType;
    }

    public String getPk() {
        return pk;
    }

    public void setPk(String pk) {
        this.pk = pk;
    }

    public Map<String, String> getData() {
        return after;
    }

    public void setData(Map<String, String> after) {
        this.after = after;
    }

    public void printcol() {
        StringBuilder sb = new StringBuilder();
        sb.append("=======================\n");
        sb.append("table:");
        sb.append(table);
        sb.append(", opType:");
        sb.append(opType);
        sb.append(", pk:");
        sb.append(pk + "\n");
        for (Map.Entry<String,String> map : after.entrySet()) {
            sb.append(map.getKey() + " -> " + map.getValue() + "\n");
        }
        System.out.println(sb.toString());
    }
}
