package com.yinhai.yhdi.increment.parser;

import com.yinhai.yhdi.increment.IcrmtCost;
import com.yinhai.yhdi.increment.entity.TablePsql;
import com.yinhai.yhdi.increment.poto.SqlPoto;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GPSqlParser2 {
    private StringBuilder sb1 = new StringBuilder();
    private StringBuilder sb2 = new StringBuilder();
    private String pksql;
    private String insert = "insert into %s (%s) values (%s)";
    private String delete = "delete from %s where %s";
    private Map<String,ArrayList<String>> tableCols;
    public GPSqlParser2(Map<String,ArrayList<String>> tableCols) {
        this.tableCols = tableCols;
    }
    public Map<String,TablePsql>  file2Psql(List<SqlPoto> sqlPotos) {
        Map<String,TablePsql> psqlMap = new HashMap<>();
        SqlPoto sqlPoto;
        TablePsql psql;
        String table;
        String pkString;
        String colName;
        ArrayList<String> pkValues;
        ArrayList<String> colNames;
        ArrayList<String> colValues;
        int cnt = sqlPotos.size();
        for (int i=0;i<cnt;i++) {
            sqlPoto =  sqlPotos.get(i);
            table = sqlPoto.getTable();
            colNames = tableCols.get(table);
            //get pk col value
            String[] pks = sqlPoto.getPk().split(",");
            pkValues = new ArrayList<>(pks.length);
            for (int k=0;k<pks.length;k++) {
                pkValues.add(sqlPoto.getAfter().get(pks[k]));
            }
            //get all col values
            int colNum = colNames.size();
            colValues = new ArrayList<>(colNum);
            for (int m=0;m<colNum;m++) {
                colValues.add(sqlPoto.getAfter().get(colNames.get(m)));
            }
            //add to psqlMap
            pkString = pkValues.toString();
            if (psqlMap.containsKey(table)) {
                psql = psqlMap.get(table);
                if (sqlPoto.getOpType().equals(IcrmtCost.OP_TYPE_I)) {
                    psql.getIdata().put(pkString,colValues);
                }else if (sqlPoto.getOpType().equals(IcrmtCost.OP_TYPE_U)) {
                    psql.getIdata().put(pkString,colValues);
                    psql.getDdata().put(pkString,pkValues);
                }else {
                    psql.getDdata().put(pkString,pkValues);
                }

            }else {
                TablePsql tablePsql = new TablePsql(table);
                pksql = "";
                sb1.delete(0,sb1.length());
                sb2.delete(0,sb2.length());
                for (int n=0;n<colNum;n++) {
                    colName = colNames.get(n);
                    sb1.append(",").append(colName);
                    sb2.append(",?");
                    if (sqlPoto.getPk().contains(colName)) {
                        pksql = pksql + " and " + colName + " = ?";
                    }
                    if (sqlPoto.getOpType().equals(IcrmtCost.OP_TYPE_I)) {
                        tablePsql.getIdata().put(pkString,colValues);
                    }else if (sqlPoto.getOpType().equals(IcrmtCost.OP_TYPE_U)) {
                        tablePsql.getIdata().put(pkString,colValues);
                        tablePsql.getDdata().put(pkString,pkValues);
                    }else {
                        tablePsql.getDdata().put(pkString,pkValues);
                    }

                }
                tablePsql.setIsql(String.format(insert,table,sb1.toString().substring(1)
                        ,sb2.toString().substring(1)));
                tablePsql.setDsql(String.format(delete,table,pksql.substring(5)));
                psqlMap.put(table,tablePsql);
            }


        }
        return psqlMap;

    }
}
