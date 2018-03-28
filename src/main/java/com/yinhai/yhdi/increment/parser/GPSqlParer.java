package com.yinhai.yhdi.increment.parser;

import com.yinhai.yhdi.increment.poto.SqlPoto;

import java.util.Map;

public class GPSqlParer {
    private StringBuilder sb1 = new StringBuilder();
    private StringBuilder sb2 = new StringBuilder();
    private String insert = "insert into %s (%s) values (%s)";
    public String file2GpSql(SqlPoto sqlPoto) {
        sb1.delete(0,sb1.length());
        sb2.delete(0,sb2.length());
        switch (sqlPoto.getOpType()) {
            case "I" :
                return getInsertSql(sqlPoto);
            case "U" :
                return getInsertSql(sqlPoto);
            case "D" : break;
        }
        return null;

    }

    private String getInsertSql(SqlPoto sqlPoto) {
        int i = 0;
        for (Map.Entry<String,String> map : sqlPoto.getAfter().entrySet()) {
            if (i > 0) {
                sb1.append(",").append(map.getKey());
                sb2.append(",").append(map.getValue());
            }else {
                sb1.append(map.getKey());
                sb2.append(map.getValue());
            }
            i ++;
        }
        return String.format(insert,sqlPoto.getTable(),sb1.toString(),sb2.toString());

    }
}
