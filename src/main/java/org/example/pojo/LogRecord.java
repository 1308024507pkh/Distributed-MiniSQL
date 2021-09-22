package org.example.pojo;

import java.util.LinkedList;

public class LogRecord {
	CmdType type;
	String SQL;
	String TableName;
	LinkedList<Integer> server_id_list;
	public LogRecord(CmdType type, String SQL, String TableName, LinkedList<Integer> server_id_list) {
		this.type = type;
		this.SQL = SQL;
		this.TableName = TableName;
		this.server_id_list = server_id_list;
	}
	@Override
	public String toString() {
		String s = type.toString() + "$" + TableName + "$" + SQL + "$";
		if(server_id_list.size()>0){
			s += server_id_list.get(0);
			int size = server_id_list.size();
			for(int i=1; i<size; i++) {
				s = s + "|" + server_id_list.get(i);
			}
		}
		s += "\n";
		return s;
	}
}


class RedoLogRecord extends LogRecord{
	private int id;
	Region region;
	public RedoLogRecord(CmdType type, String SQL, String TableName, LinkedList<Integer> server_id_list, int id, Region region) {
		super(type, SQL, TableName, server_id_list);
		this.id = id;
		this.region = region;
	}
	public RedoLogRecord(LogRecord log, int id, Region region) {
		this(log.type, log.SQL, log.TableName, log.server_id_list, id, region);
	}
	public Region getRegion() {
		return region;
	}
	public int getId() {
		return id;
	}
	
}