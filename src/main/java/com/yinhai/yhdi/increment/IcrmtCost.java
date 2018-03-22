package com.yinhai.yhdi.increment;

public interface IcrmtCost {
    String START_LMNR_SQL="BEGIN SYS.DBMS_LOGMNR.start_logmnr(startScn => %s , Options => %s );END;";
}
