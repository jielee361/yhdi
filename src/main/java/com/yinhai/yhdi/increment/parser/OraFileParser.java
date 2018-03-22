package com.yinhai.yhdi.increment.parser;

import com.yinhai.yhdi.increment.entity.RedoObj;

public class OraFileParser {
    public String redo2Rrecord(RedoObj redo) {
        switch (redo.getOperation_code()) {
            case 1 :
                return transInsert(redo);
            case 2 : break;
            case 3 : break;

        }
        return null;

    }

    private String transInsert(RedoObj redo) {
        String record = redo.getSql_redo().substring(redo.getSql_redo().indexOf(" values ")+9,
                redo.getSql_redo().length()-1).replaceAll("TIMESTAMP ' ","'")
                .replaceAll("[EMPTY_CLOB()]","");
        return record;

    }
}
