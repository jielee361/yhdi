package com.yinhai.yhdi.increment.parser;

import com.yinhai.yhdi.increment.poto.RedoObj;
import com.yinhai.yhdi.increment.poto.SqlPoto;

public class OraFileParser {
    private int inp;
    private String vs1;
    private int dot;
    private String colvalue;
    private String colname;

    public SqlPoto redo2Poto(RedoObj redo) {
        SqlPoto sqlPoto = new SqlPoto();
        sqlPoto.setTable(redo.getTable_name());
        sqlPoto.setPk("");
        switch (redo.getOperation_code()) {
            case 1 :
                sqlPoto.setOpType("I");
                transInsertJson(sqlPoto,redo.getSql_redo());
                break;
            case 2 :
                sqlPoto.setOpType("D");
                transDeleteJson(sqlPoto,redo.getSql_redo());
                break;
            case 3 :
                sqlPoto.setOpType("U");
                transUpdateJson(sqlPoto,redo.getSql_redo());
                break;
        }
        sqlPoto.printcol();
        return sqlPoto;
    }
    private void transDeleteJson(SqlPoto sqlPoto,String sqlRedo) {
        //delete from "HSTEST"."TB_TEST1" where "N1" = 11231243432342344 and "N2" = 3462773451.123456
        // and "V1" = '测试V\n‘’' and "V2" = '1' and "C1" = 'cc      '
        vs1 = sqlRedo.substring(sqlRedo.indexOf(" where \"") + 8) + " and \""; //no risk
        while (vs1.length() > 1) {
            colname = vs1.substring(0,vs1.indexOf("\""));//no risk
            vs1 = vs1.substring(vs1.indexOf("\"") + 4);//no risk
            dot = vs1.indexOf(" and \"") + 6;
            if (vs1.startsWith("NULL") || vs1.startsWith("EMPTY") || vs1.startsWith(" NULL")) {//空值 no risk
                colvalue = "null";
                vs1 = vs1.substring(dot);
            }else if (vs1.startsWith("TIMESTAMP")) {//时间日期 //no risk
                colvalue = vs1.substring(12,vs1.indexOf("'",13));
                vs1 = vs1.substring(dot);
            }else if (vs1.startsWith("'")) {
                colvalue = vs1.substring(1,vs1.indexOf("' and"));//no risk
                vs1 = vs1.substring(vs1.indexOf("' and") + 7);
            }else {
                colvalue = vs1.substring(0,dot - 6);
                vs1 = vs1.substring(dot);
            }
            sqlPoto.putCol(colname,colvalue);
        }
    }

    private void transUpdateJson(SqlPoto sqlPoto,String sqlRedo) {
        //update "HSTEST"."TB_TEST1" set "N2" = 12323, "V1" = 'dsfd'',saf', "D1" = TIMESTAMP ' 2018-03-06 11:28:49',
        // "T1" = TIMESTAMP ' 2017-10-23 12:15:00.123000' where "N1" = 112312435323
        transDeleteJson(sqlPoto,sqlRedo);
        vs1 = sqlRedo.substring(sqlRedo.indexOf(" set ") + 5,sqlRedo.indexOf(" where \"")) + ", \"";
        while (vs1.length() > 1) {
            colname = vs1.substring(1,vs1.indexOf(" =")).replace("\"","");//no risk
            vs1 = vs1.substring(vs1.indexOf("= ")+2);//no risk
            dot = vs1.indexOf(", \"") + 2;
            if (vs1.startsWith("NULL") || vs1.startsWith("EMPTY")) {//空值 no risk
                colvalue = "null";
                vs1 = vs1.substring(dot);
            }else if (vs1.startsWith("TIMESTAMP")) {//时间日期 //no risk
                colvalue = vs1.substring(12,vs1.indexOf("'",13));
                vs1 = vs1.substring(dot);
            }else if (vs1.startsWith("'")) {
                if (vs1.contains("', \"")) {//have risk: "', ""
                    colvalue = vs1.substring(1,vs1.indexOf("', \""));//have risk: "', ""
                    vs1 = vs1.substring(vs1.indexOf("', \"") + 3); //have risk: "', "" --no
                }else { //最后一个字段
                    colvalue = vs1.substring(1,vs1.length() - 4);
                    vs1 = "";
                }
            }else {
                colvalue = vs1.substring(0,dot-2);
                vs1 = vs1.substring(dot);
            }
            sqlPoto.putCol(colname,colvalue);

        }
    }

    private void transInsertJson(SqlPoto sqlPoto,String sqlRedo) {
        //) values (112312435323425479,'a"dfs",f',NULL,'12','cc      ',EMPTY_CLOB(),TIMESTAMP ' 2018-03-22 11:28:49',
        // TIMESTAMP ' 2017-12-23 12:15:00.123000',TIMESTAMP ' 2017-12-23 12:15:00.123456780')

        inp = sqlRedo.indexOf(") values (");//no risk
        String[] cols = sqlRedo.substring(sqlRedo.indexOf("(")+1,inp).replace("\"","").split(",");//no risk
        int size = cols.length;
        vs1 = sqlRedo.substring(inp + 10);
        for (int i=0;i<size;i++) {
            dot = vs1.indexOf(",") + 1;//取colvalue不能用dot因为dot有可能为0--最后一个字段//no risk
            if (vs1.startsWith("NULL") || vs1.startsWith("EMPTY")) {//空值
                colvalue = "null";
                vs1 = vs1.substring(dot);
            }else if (vs1.startsWith("TIMESTAMP")) {//时间日期
                colvalue = vs1.substring(12,vs1.indexOf("'",13));
                vs1 = vs1.substring(dot);
            }else if (vs1.startsWith("'")) {
                if (i == size - 1) {//最后一个字段
                    colvalue = vs1.substring(1,vs1.length()-2);
                }else {
                    colvalue = vs1.substring(1,vs1.indexOf("',")-1);//have risk: "',"
                    vs1 = vs1.substring(vs1.indexOf("',")+2); //have risk: ', --no
                }
            }else {
                colvalue = vs1.substring(0,dot-1);
                vs1 = vs1.substring(dot);
            }
            sqlPoto.putCol(cols[i],colvalue);
        }
    }
}
