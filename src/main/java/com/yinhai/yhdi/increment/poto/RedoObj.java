package com.yinhai.yhdi.increment.poto;

import java.io.Serializable;

public class RedoObj implements Serializable {

	private static final long serialVersionUID = 1L;
	private int operation_code;
	private String seg_owner;
	private String table_name;
	private long scn;
	private String rs_id;
	private int ssn;
	private String sql_redo;
	private String row_id;
	private String src_con_name;

	public int getOperation_code() {
		return operation_code;
	}
	public void setOperation_code(int operation_code) {
		this.operation_code = operation_code;
	}

	public String getSeg_owner() {
		return seg_owner;
	}

	public void setSeg_owner(String seg_owner) {
		this.seg_owner = seg_owner;
	}

	public String getTable_name() {
		return table_name;
	}

	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}

	public long getScn() {
		return scn;
	}

	public void setScn(long scn) {
		this.scn = scn;
	}

	public String getRs_id() {
		return rs_id;
	}

	public void setRs_id(String rs_id) {
		this.rs_id = rs_id;
	}

	public int getSsn() {
		return ssn;
	}

	public void setSsn(int ssn) {
		this.ssn = ssn;
	}

	public String getSql_redo() {
		return sql_redo;
	}

	public void setSql_redo(String sql_redo) {
		this.sql_redo = sql_redo;
	}

	public String getRow_id() {
		return row_id;
	}

	public void setRow_id(String row_id) {
		this.row_id = row_id;
	}

	public String getSrc_con_name() {
		return src_con_name;
	}

	public void setSrc_con_name(String src_con_name) {
		this.src_con_name = src_con_name;
	}

	public String keyString() {
		return new StringBuffer()
				.append(src_con_name == null ? "" : src_con_name.toUpperCase()
						+ "-").append(seg_owner.toUpperCase()).append("-")
				.append(table_name.toUpperCase()).toString();
	}

	public String scheduleString() {
		return new StringBuffer().append(scn).append("-").append(rs_id)
				.append("-").append(ssn).toString();
	}
}
