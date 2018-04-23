package com.yinhai.yhdi.increment.parser;

import com.yinhai.yhdi.increment.poto.SqlPoto;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveParser {
    private Map<String, ArrayList<String>> tableMeta;
    private StringBuilder sb1 = new StringBuilder();
    private long currentNum;
    public HiveParser(Map<String, ArrayList<String>> tableMeta) {
        this.tableMeta = tableMeta;

    }
    public Map<String,ArrayList<String>> file2HiveFile(List<SqlPoto> sqlPotos) {
        Map<String,ArrayList<String>> psqlMap = new HashMap<>();
        currentNum = System.currentTimeMillis()/100*1000;
        SqlPoto sqlPoto;
        String table;
        ArrayList<String> colNames;
        int colNum;
        int cnt = sqlPotos.size();
        for (int i=0;i<cnt;i++) {//每条记录
            sqlPoto =  sqlPotos.get(i);
            table = sqlPoto.getTable();
            colNames = tableMeta.get(table);
            if (colNames == null) {
                throw new RuntimeException("目标端出错! 目标端未获取到表：" + table + " 的字段信息，请确认表是否存在。");
            }
            //get all col values
            colNum = colNames.size();
            sb1.delete(0,sb1.length());
            for (int m=0;m<colNum;m++) { //每个字段
                sb1.append(sqlPoto.getAfter().get(colNames.get(m)).replaceAll("\n",""));
                sb1.append("\t");
            }
            //标记字段
            sb1.append(sqlPoto.getOpType()).append("\t");//chg_type
            sb1.append(currentNum + i).append("\n");//time
            //add to psqlMap
            if (psqlMap.containsKey(table)) {
                psqlMap.get(table).add(sb1.toString());
            }else {
                ArrayList<String> records = new ArrayList<>();
                records.add(sb1.toString());
                psqlMap.put(table,records);
                }
            }

        return psqlMap;

    }
}
