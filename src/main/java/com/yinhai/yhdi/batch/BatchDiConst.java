package com.yinhai.yhdi.batch;

public interface BatchDiConst {
    //数据库类型
    String DB_KIND_HIVE = "hive";
    String DB_KIND_ORACLE="oracle";
    String DB_KIND_CARBON="carbondata";
    //运行状态
    int RUN_STAT_QUEUE=1;
    int RUN_STAT_RUNNING=2;
    int RUN_STAT_SUCCESS=3;
    int RUN_STAT_FAIL=4;

    String EXTRACT_SQL_ORA1 = "select * from %s";
    String EXTRACT_SQL_ORA2 = "select * from %s where rowid between %s";

    String EXTRACT_STAT_OUTP = "task: %s -> 状态：%s, 数据条数：%s, 时长：%s 秒";


    String GET_PART_SQL = "SELECT dbms_rowid.rowid_create(1, DOI, lo_fno, lo_block, 0)||'field' ||dbms_rowid.rowid_create(1, DOI, hi_fno, hi_block, 1000000) partsql \n"
            + "FROM (SELECT DISTINCT DOI,grp, \n"
            + "first_value(relative_fno) over(PARTITION BY DOI, grp ORDER BY relative_fno, block_id rows BETWEEN unbounded preceding AND unbounded following) lo_fno, \n"
            + "first_value(block_id) over(PARTITION BY DOI, grp ORDER BY relative_fno, block_id rows BETWEEN unbounded preceding AND unbounded following) lo_block, \n"
            + "last_value(relative_fno) over(PARTITION BY DOI, grp ORDER BY relative_fno, block_id rows BETWEEN unbounded preceding AND unbounded following) hi_fno, \n"
            + "last_value(block_id + blocks - 1) over(PARTITION BY DOI, grp ORDER BY relative_fno, block_id rows BETWEEN unbounded preceding AND unbounded following) hi_block, \n"
            + "sum(blocks) over(PARTITION BY DOI, grp) sum_blocks,SUBOBJECT_NAME FROM  \n"
            + "(SELECT obj.OBJECT_ID,obj.SUBOBJECT_NAME,obj.DATA_OBJECT_ID AS DOI,ext.relative_fno,ext.block_id,SUM(blocks) over() SUM, \n"
            + "SUM(blocks) over(ORDER BY DATA_OBJECT_ID, relative_fno, block_id) - 0.01 sum_fno,TRUNC((SUM(blocks) over(ORDER BY DATA_OBJECT_ID, \n"
            + "relative_fno,block_id) - 0.01) / (SUM(blocks) over() / %s)) grp, ext.blocks FROM dba_extents ext, dba_objects obj \n"
            + "WHERE ext.segment_name = '%s' AND ext.owner = '%s' AND obj.owner = ext.owner AND obj.object_name = ext.segment_name \n"
            + "AND obj.DATA_OBJECT_ID IS NOT NULL ORDER BY DATA_OBJECT_ID, relative_fno, block_id) ORDER BY DOI, grp)";


}
