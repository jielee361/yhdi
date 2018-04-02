package com.yinhai.yhdi.increment.read;

import com.yinhai.yhdi.common.DiPrp;
import com.yinhai.yhdi.increment.IcrmtCost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class OraLogmnrOper {
	private static final Logger logger = LoggerFactory.getLogger(OraLogmnrOper.class);

	/**
	 * Get the available dictionary from redolog before the startScn
	 * 
	 * @param conn
	 * @param startScn
	 * @return
	 * @throws SQLException
	 */
//	public Long getDicScn(Connection conn, Long startScn) throws SQLException {
//		String sql = "SELECT first_change# FROM v$archived_log WHERE sequence# = (SELECT MAX(sequence#) FROM v$archived_log WHERE dictionary_begin = 'YES' AND STATUS='A' AND first_change#<="
//				+ startScn + ")";
//		Statement stmt = conn.createStatement();
//		ResultSet rs = stmt.executeQuery(sql);
//		if (rs.next()) {
//			Long scn = rs.getLong(1);
//			System.out.println("Begin_SCN:[" + startScn + "],已找到字典SCN:[" + scn + "]");
//			return scn;
//		} else {
//			throw new RuntimeException("未找到抽取进度之前的有效字典");
//		}
//	}

	/**
	 * 启动LOGMINOR挖掘。
	 * 
	 * @param conn
	 * @param startScn
	 *            起始SNC号
	 * @param options
	 *            挖掘参数
	 * @throws Exception
	 */
	public void startLogmnr(Connection conn, long startScn, String options)
			throws Exception {
		// 启动挖掘
		String setDate = "ALTER SESSION SET NLS_DATE_FORMAT = 'yyyy-mm-dd hh24:mi:ss'";
		String setTime = "alter session set NLS_TIMESTAMP_FORMAT='yyyy-mm-dd hh24:mi:ss.ff'";
		//String setTime = "alter session set nls_date_language='american'";
		String wjsql = String.format(IcrmtCost.START_LMNR_SQL,startScn,options);
		logger.info("LOGMNR挖掘SQL：" + wjsql);
		Statement st = conn.createStatement();
		st.execute(setDate);
		st.execute(setTime);
		st.close();
		CallableStatement cs = conn.prepareCall(wjsql);
		cs.execute();
		logger.info("LOGMNR挖掘启动完成！" );
	}

	/**
	 * 获取日志挖掘结果集
	 * @param conn
	 * @param beginScn
	 * @param tableString 格式（db1.t1,db1.t1） 12c: (pdb1.db1.t1,pdb1.db1.t2)
	 * @param hasContainer 是否有PDB，及是否是12C
	 * @param sqlKind 操作类型，(1,2,3,4)
	 * @return
	 * @throws SQLException
	 */
 	public ResultSet getLogmnrResult(Connection conn, Long beginScn,
			String tableString, Boolean hasContainer,String sqlKind) throws SQLException {
		String getsql = null;
		if (hasContainer) {
			getsql = new StringBuffer()
					.append("select scn,rs_id,ssn,seg_owner,table_name,operation_code,sql_redo,csf,src_con_name from v$logmnr_contents a where a.SCN>=")
					.append(beginScn.toString())
					.append(" and src_con_name = '")
					.append(DiPrp.getProperty("ora12c.pdbname").toUpperCase())
					.append("' and seg_owner||'.'||table_name in (")
					.append(tableString)
					.append(") and operation_code in ")
					.append(sqlKind)
					.toString();
		} else {
			getsql = new StringBuffer()
					.append("select scn,rs_id,ssn,seg_owner,table_name,operation_code,sql_redo,csf from v$logmnr_contents a where a.SCN>=")
					.append(beginScn.toString())
					.append(" and seg_owner||'.'||table_name in (")
					.append(tableString)
					.append(") and operation_code in ")
					.append(sqlKind).toString();
		}
		logger.info("增量抽取抽取SQL:" + getsql);
		PreparedStatement ps = conn.prepareStatement(getsql);
		ps.setFetchSize(1);
		ResultSet rs = ps.executeQuery();
		logger.info("抽取过滤完成！" );
		return rs;
	}
}
