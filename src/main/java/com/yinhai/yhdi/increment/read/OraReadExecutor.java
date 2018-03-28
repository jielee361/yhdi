package com.yinhai.yhdi.increment.read;

import com.alibaba.fastjson.JSONObject;
import com.yinhai.yhdi.common.CommonConn;
import com.yinhai.yhdi.increment.IcrmtEnv;
import com.yinhai.yhdi.increment.entity.IcrmtConf;
import com.yinhai.yhdi.increment.poto.RedoObj;
import com.yinhai.yhdi.increment.parser.OraSqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.concurrent.LinkedBlockingDeque;

public class OraReadExecutor extends ReadExecutor {
    private long lastScn;
    private String lastRsid;
    private int lastSsn;
    private long beginScn;
    private boolean stopFlag;
    private int queueMaxSize;
    private IcrmtConf icrmtConf;
    private Connection conn;
    private static final Logger logger = LoggerFactory.getLogger(OraReadExecutor.class);

    public OraReadExecutor(Connection conn) {
        icrmtConf = IcrmtEnv.getIcrmtConf();
        this.queueMaxSize = icrmtConf.getRedoQueueSize();
        this.conn = conn;
        this.stopFlag = false;
    }

    @Override
    public void stopRead() {
        this.stopFlag = true;
    }

    @Override
    public void startRead(LinkedBlockingDeque<RedoObj> redoQueue) throws Exception {
        this.lastScn = IcrmtEnv.getLastIndex().getScn();
        this.lastRsid = IcrmtEnv.getLastIndex().getRsid();
        this.lastSsn = IcrmtEnv.getLastIndex().getSsn();
        if (this.lastScn == 0L) { //首次启动
            beginScn = icrmtConf.getLgmnrBeginScn();
        }else { //恢复启动
            beginScn = lastScn;
        }
        //start lgmnr
        OraLogmnrOper oraLgmnrOper = new OraLogmnrOper();
        oraLgmnrOper.startLogmnr(conn,beginScn,icrmtConf.getLgmnrOpertion());
        //start read
        ResultSet rs = oraLgmnrOper.getLogmnrResult(conn, beginScn,
                icrmtConf.getSourceTable(), icrmtConf.isOracle12c(), icrmtConf.getLgmnrSqlkind());
        StringBuffer sqlStrBuff = new StringBuffer();
        OraSqlParser oraSqlParser = new OraSqlParser();
        //先衔接
        while (rs.next() && !stopFlag) {
            if (rs.getLong(1) <= this.lastScn) {
                if (rs.getLong(1) == this.lastScn && rs.getString(2).equals(this.lastRsid)
                        && rs.getInt(3) == this.lastSsn) {
                    break;//找到了上次抽取断点，跳出本次循环，进入后续正常抽取
                }

            }else {
                //本条记录已大于LastScn，需要抽取，抽取后进行后续正常抽取。
                if (rs.getInt(8) == 0) {
                    RedoObj redoObj = new RedoObj();
                    redoObj.setScn(rs.getLong(1));
                    redoObj.setRs_id(rs.getString(2).trim());
                    redoObj.setSsn(rs.getInt(3));
                    redoObj.setSeg_owner(rs.getString(4));
                    redoObj.setTable_name(rs.getString(5));
                    redoObj.setOperation_code(rs.getInt(6));
                    redoObj.setSql_redo(sqlStrBuff.append(rs.getString(7)).toString());
                    redoQueue.add(redoObj);
                    sqlStrBuff.delete(0, sqlStrBuff.length());
                } else {
                    sqlStrBuff.append(rs.getString(7));
                }
                break;//进入正常抽取
            }
        }
        //再正常抽取
        while (rs.next() && !stopFlag) {
            if (rs.getInt(8) == 0) {
                RedoObj redoObj = new RedoObj();
                redoObj.setScn(rs.getLong(1));
                redoObj.setRs_id(rs.getString(2).trim());
                redoObj.setSsn(rs.getInt(3));
                redoObj.setSeg_owner(rs.getString(4));
                redoObj.setTable_name(rs.getString(5));
                redoObj.setOperation_code(rs.getInt(6));
                redoObj.setSql_redo(sqlStrBuff.append(rs.getString(7)).toString());
                redoQueue.add(redoObj);
                sqlStrBuff.delete(0, sqlStrBuff.length());
                if (redoQueue.size() > queueMaxSize) {
                    logger.info("缓存队列出现积压，暂停1秒后再抽取！");
                    Thread.sleep(1000L);
                }
            } else {
                sqlStrBuff.append(rs.getString(7));
            }
        }
        CommonConn.closeRS(rs);
    }
}
