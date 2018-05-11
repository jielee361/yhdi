package com.yinhai.yhdi.increment.parser;

import com.alibaba.fastjson.JSONObject;
import com.yinhai.yhdi.increment.poto.RedoObj;

public class OraSqlParser extends SqlParser{
    private String table = "table";
    private String opType = "op_type";
    private String pk = "pk";
    private String after = "after";
    private String before = "before";
    private int inp;
    private String vs1;
    private int dot;
    private String colvalue;
    private String colname;

    @Override
    public JSONObject redo2Json(RedoObj redo) {
        JSONObject sqlJson = new JSONObject(4);
        sqlJson.put(table,redo.getTable_name());
        sqlJson.put(pk,"");
        switch (redo.getOperation_code()) {
            case 1 :
                sqlJson.put(opType,"I");
                sqlJson.put(after,transInsertJson(redo.getSql_redo()));
                break;
            case 2 :
                sqlJson.put(opType,"D");
                sqlJson.put(before,transDeleteJson(redo.getSql_redo()));
                break;
            case 3 :
                sqlJson.put(opType,"U");
                sqlJson.put(after,transUpdateJson(redo.getSql_redo()));
                break;
        }

        return sqlJson;
    }
    private JSONObject transDeleteJson(String sqlRedo) {
        //delete from "HSTEST"."TB_TEST1" where "N1" = 11231243432342344 and "N2" = 3462773451.123456
        // and "V1" = '测试V\n‘’' and "V2" = '1' and "C1" = 'cc      '
        JSONObject colJson = new JSONObject();
        vs1 = sqlRedo.substring(sqlRedo.indexOf(" where \"") + 8) + " and \""; //no risk
        while (vs1.length() > 1) {
            colname = vs1.substring(0,vs1.indexOf("\""));//no risk
            vs1 = vs1.substring(vs1.indexOf("\"") + 4);//no risk
            dot = vs1.indexOf(" and \"") + 6;
            if (vs1.startsWith("NULL") || vs1.startsWith("EMPTY") || vs1.startsWith(" NULL")) {//空值 no risk
                colvalue = "";
                vs1 = vs1.substring(dot);
            }else if (vs1.startsWith("TIMESTAMP")) {//时间日期 //no risk
                colvalue = vs1.substring(12,vs1.indexOf("'",13));
                vs1 = vs1.substring(dot);
            }else if (vs1.startsWith("'")) {
                colvalue = vs1.substring(1,vs1.indexOf("' and"));//no risk
                vs1 = vs1.substring(vs1.indexOf("' and") + 7);
                //System.out.println(vs1);
            }else {
                colvalue = vs1.substring(0,dot - 6);
                vs1 = vs1.substring(dot);
            }
            colJson.put(colname,colvalue);
        }
        return colJson;
    }

    private JSONObject transUpdateJson(String sqlRedo) {
        //update "HSTEST"."TB_TEST1" set "N2" = 12323, "V1" = 'dsfd'',saf', "D1" = TIMESTAMP ' 2018-03-06 11:28:49',
        // "T1" = TIMESTAMP ' 2017-10-23 12:15:00.123000' where "N1" = 112312435323
        JSONObject colJson = transDeleteJson(sqlRedo);
        vs1 = sqlRedo.substring(sqlRedo.indexOf(" set ") + 5,sqlRedo.indexOf(" where \"")) + ", \"";
        while (vs1.length() > 1) {
            colname = vs1.substring(1,vs1.indexOf(" =")).replace("\"","");//no risk
            vs1 = vs1.substring(vs1.indexOf("= ")+2);//no risk
            dot = vs1.indexOf(", \"") + 2;
            if (vs1.startsWith("NULL") || vs1.startsWith("EMPTY")) {//空值 no risk
                colvalue = "";
                vs1 = vs1.substring(dot);
            }else if (vs1.startsWith("TIMESTAMP")) {//时间日期 //no risk
                colvalue = vs1.substring(12,vs1.indexOf("'",13));
                vs1 = vs1.substring(dot);
            }else if (vs1.startsWith("'")) {
                if (vs1.contains("', \"")) {//have risk: "', ""
                    colvalue = vs1.substring(1,vs1.indexOf("', \""));//have risk: "', ""
                    vs1 = vs1.substring(vs1.indexOf("', \"") + 3); //have risk: "', "" --no
                }else { //最后一个字段
                    colvalue = vs1.substring(0,vs1.length() - 3);
                    vs1 = "";
                }
            }else {
                colvalue = vs1.substring(0,dot-2);
                vs1 = vs1.substring(dot);
            }
            colJson.put(colname,colvalue);

        }
        return colJson;
    }

    private JSONObject transInsertJson(String sqlRedo) {
        //) values (112312435323425479,'a"dfs",f',NULL,'12','cc      ',EMPTY_CLOB(),TIMESTAMP ' 2018-03-22 11:28:49',
        // TIMESTAMP ' 2017-12-23 12:15:00.123000',TIMESTAMP ' 2017-12-23 12:15:00.123456780')

        inp = sqlRedo.indexOf(") values (");//no risk
        String[] cols = sqlRedo.substring(sqlRedo.indexOf("(")+1,inp).replace("\"","").split(",");
        int size = cols.length;
        JSONObject colJson = new JSONObject(size);
        vs1 = sqlRedo.substring(inp + 10);
        for (int i=0;i<size;i++) {
            dot = vs1.indexOf(",") + 1;//取colvalue不能用dot因为dot有可能为0--最后一个字段//no risk
            if (vs1.startsWith("NULL") || vs1.startsWith("EMPTY")) {//空值
                colvalue = "";
                vs1 = vs1.substring(dot);
            }else if (vs1.startsWith("TIMESTAMP")) {//时间日期
                colvalue = vs1.substring(12,vs1.indexOf("'",13));
                vs1 = vs1.substring(dot);
            }else if (vs1.startsWith("'")) {
                if (i == size - 1) {//最后一个字段
                    colvalue = vs1.substring(1,vs1.length()-2);
                }else {
                    colvalue = vs1.substring(1,vs1.indexOf("',"));//have risk: "',"
                    vs1 = vs1.substring(vs1.indexOf("',")+2); //have risk: ', --no
                }
            }else {
                colvalue = vs1.substring(0,dot-1);
                vs1 = vs1.substring(dot);
            }
            colJson.put(cols[i].replace("\"",""),colvalue);
        }
        return colJson;
    }
}
