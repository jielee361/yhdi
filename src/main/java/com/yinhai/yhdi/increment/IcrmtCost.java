package com.yinhai.yhdi.increment;

public interface IcrmtCost {
    String START_LMNR_SQL="BEGIN SYS.DBMS_LOGMNR.start_logmnr(startScn => %s , Options => %s );END;";
    String ORA_PK_SQL="select cu.* from user_cons_columns cu, user_constraints au where cu.constraint_name"
            + " = au.constraint_name and au.constraint_type = 'P' and cu.owner||'.'||cu.table_name in (%s) ";
}
