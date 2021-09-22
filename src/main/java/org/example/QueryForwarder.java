package org.example;

import java.util.LinkedList;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.example.pojo.*;

public class QueryForwarder{
	Master master;
	public void setMaster(Master m) {
		this.master = m;
	}
	CmdResultCode CREATE_TABLE(CmdType type, String SQL, String TableName){
		synchronized (master.getRegions()){
			// 1.验证是否表是否已存在
			for(Region region : master.getRegions().values()){
				if(region.getTableName().equals(TableName)) {
					return CmdResultCode.TableAlreadyExist;
				}
			}
			// 2.分配该表应在的RegionServer
			LinkedList<RegionServerMetadata> rs_list = master.findNewServer();
			// 3.向分配到的RegionServer转发SQL指令
			CmdResultCode result = CmdResultCode.UnknownError;
			LinkedList<Integer> server_id_list = new LinkedList<Integer>();
			result = CallRegionServerInterface(type, rs_list, server_id_list, SQL);
			// 4.获取SQL指令执行结果及成功执行的RegionServer列表，若至少一个Region Server成功执行，更改分区信息，生成日志记录
			if(result==CmdResultCode.Success) {
				ChangeDistributionInfo(type, TableName, server_id_list, rs_list, false);
				LogRecord logRecord = new LogRecord(type, SQL, TableName, server_id_list);
				master.addLogRecord(logRecord);
				master.getLM().flush();
			}
			return result;
		}
	}
	CmdResultCode DROP_TABLE(CmdType type, String SQL, String TableName){
		// 找到表所对应的RegionID
		int RegionID = SearchRegionID(TableName);
		if(RegionID==-1) return CmdResultCode.TableNotExist;
		// 调用EXECUTE方法，转发SQL语句，获取执行结果，更新分区信息及日志
		return EXECUTE(type, RegionID, TableName, SQL);
	}
	CmdResultCode CREATE_INDEX(CmdType type, String SQL, String TableName){
		// 找到表所对应的RegionID
		int RegionID = SearchRegionID(TableName);
		if(RegionID==-1) return CmdResultCode.TableNotExist2;
		// 调用EXECUTE方法，转发SQL语句，获取执行结果，更新日志
		return EXECUTE(type, RegionID, TableName, SQL);
	}
	CmdResultCode DROP_INDEX(CmdType type, String SQL, String TableName){
		// 同上
		int RegionID = SearchRegionID(TableName);
		if(RegionID==-1) return CmdResultCode.TableNotExist3;
		return EXECUTE(type, RegionID, TableName, SQL);
	}
	CmdResultCode INSERT(CmdType type, String SQL, String TableName) {
		// 同上
		int RegionID = SearchRegionID(TableName);
		if(RegionID==-1) return CmdResultCode.TableNotExist4;
		return EXECUTE(type, RegionID, TableName, SQL);
	}
	CmdResultCode DELETE(CmdType type, String SQL, String TableName) {
		// 同上
		int RegionID = SearchRegionID(TableName);
		if(RegionID==-1) return CmdResultCode.TableNotExist5;
		return EXECUTE(type, RegionID, TableName, SQL);
	}
	int SearchRegionID(String TableName) {
		// 找到表名所对应的Region ID，未找到则返回负1
		synchronized (master.getRegions()){
			for(int id : master.getRegions().keySet()){
				Region region = master.getRegions().get(id);
				if(region.getTableName().equals(TableName)) {
					return id;
				}
			}
			return -1;
		}
	}
	
	CmdResultCode EXECUTE(CmdType type, int regionID, String TableName, String SQL) {
		CmdResultCode result = CmdResultCode.UnknownError;
		Region r = master.getRegions().get(regionID);
		LinkedList<Integer> server_id_list = new LinkedList<Integer>();
		// 转发SQL给该表所在的Region Server执行
		if(r.getTableName().equals(TableName)) {
			result = CallRegionServerInterface(type, r, server_id_list, SQL);
		}
		//执行成功
		if(result==CmdResultCode.Success) {
			// 若是删表SQL，则要更新分区信息
			if(type==CmdType.DROP_TABLE) {
				LinkedList<RegionServerMetadata> rs_list = new LinkedList<RegionServerMetadata>();
				for(int i=0; i<r.getDuplicates().size(); i++) {
					rs_list.add(master.getRegionServers().get(r.getDuplicates().get(i).getServer_id()));
				}
				ChangeDistributionInfo(type, TableName, server_id_list, rs_list);
			}
			// 生成日志
			LogRecord logRecord = new LogRecord(type, SQL, TableName, server_id_list);
			master.addLogRecord(logRecord);
			master.getLM().flush();
		}
		return result;
	}
	
	public CmdResultCode CallRegionServerInterface(CmdType type, LinkedList<RegionServerMetadata> rs_list, LinkedList<Integer> server_id_list, String SQL) {
		CmdResultCode result = CmdResultCode.UnknownError;
		LinkedList<CmdResultCodePack> resultCodePacks = new LinkedList<QueryForwarder.CmdResultCodePack>();
		LinkedList<Thread> threads = new LinkedList<Thread>();
		// 为每一个被分配的的Region Server，生成一个线程转发SQL语句并获取执行结果
		for(int i = rs_list.size()-1; i>=0; i--) {
			CmdResultCodePack resultCodePack = new CmdResultCodePack();
			resultCodePacks.add(resultCodePack);
			int serverId = rs_list.get(i).getId();
			Thread thread = new Thread(new CallThriftThread(type, resultCodePack, serverId, SQL));
			threads.add(thread);
			thread.start();
		}
		for(int i = rs_list.size()-1; i>=0; i--) {
			try {
				threads.get(i).join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		// 统计执行成功的Region Server的列表，
		// 若至少在一个Region Server执行成功，则认定该SQL语句执行成功，返回给上层，未成功的由日志系统修复，
		for(int i = rs_list.size()-1; i>=0; i--) {
			if(resultCodePacks.get(i).getResult() == CmdResultCode.Success) {
				int serverId = rs_list.get(rs_list.size()-1-i).getId();
				server_id_list.add(serverId);
				result = CmdResultCode.Success;
			}
		}
		return result;
	}
	
	public CmdResultCode CallRegionServerInterface(CmdType type, Region r, LinkedList<Integer> server_id_list, String SQL){
		CmdResultCode result = CmdResultCode.UnknownError;
		LinkedList<CmdResultCodePack> resultCodePacks = new LinkedList<QueryForwarder.CmdResultCodePack>();
		LinkedList<Thread> threads = new LinkedList<Thread>();
		LinkedList<Integer> server_id_list_called = new LinkedList<Integer>();
		// 为每一个副本的Region Server，生成一个线程转发SQL语句并获取执行结果
		for(int i = r.getDuplicates().size()-1; i>=0; i--) {
			int sid = r.getDuplicates().get(i).getServer_id();
			if(server_id_list.contains(sid)) {
				continue;
			}
			server_id_list_called.add(sid);
			CmdResultCodePack resultCodePack = new CmdResultCodePack();
			resultCodePacks.add(resultCodePack);
			int serverId = r.getDuplicates().get(i).getServer_id();
			Thread thread = new Thread(new CallThriftThread(type, resultCodePack, serverId, SQL));
			threads.add(thread);
			thread.start();
		}
		for(int i = resultCodePacks.size()-1; i>=0; i--) {
			try {
				threads.get(i).join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		// 统计执行成功的Region Server的列表，
		// 若至少在一个Region Server执行成功，则认定该SQL语句执行成功，返回给上层，未成功的由日志系统修复，
		for(int i = resultCodePacks.size()-1; i>=0; i--) {
			if(resultCodePacks.get(i).getResult() == CmdResultCode.Success) {
				int serverId = server_id_list_called.get(i);
				server_id_list.add(serverId);
				result = CmdResultCode.Success;
			}
		}
		return result;
	}
	
	
	public void ChangeDistributionInfo(CmdType type, String TableName, LinkedList<Integer> server_id_list, LinkedList<RegionServerMetadata> rs_list) {
		// DROP_TABLE, 修改分区信息	
		master.deleteRegion(TableName, server_id_list, rs_list);
	}
	public void ChangeDistributionInfo(CmdType type, String TableName, LinkedList<Integer> server_id_list, LinkedList<RegionServerMetadata> rs_list, boolean redo) {
		// CREATE_TABLE，修改分区信息
		master.addRegion(TableName, server_id_list, rs_list, redo);
	}
	class CmdResultCodePack{
		private CmdResultCode result;
		public CmdResultCodePack() {
			result = CmdResultCode.UnknownError;
		}
		public void setResult(CmdResultCode result) {
			this.result = result;
		}
		public CmdResultCode getResult() {
			return result;
		}
	}
    public CmdType judge_sql(String query){
        if(query.indexOf("join") != -1) return CmdType.Nofunc;
        
        if(query.indexOf("select") != -1) return CmdType.SELECT;

        if(query.indexOf("create table") != -1) return CmdType.CREATE_TABLE;
        if(query.indexOf("drop table") != -1) return CmdType.DROP_TABLE;

        if(query.indexOf("create index") != -1) return CmdType.CREATE_INDEX;
        if(query.indexOf("drop index") != -1) return CmdType.DROP_INDEX;
        
        if(query.indexOf("insert") != -1) return CmdType.INSERT;
        
        if(query.indexOf("delete") != -1) return CmdType.DELETE;

        return CmdType.SyntaxError;
      }
    
	class CallThriftThread implements Runnable{
		CmdResultCodePack pack;
		int server_id;
		String SQL;
		CmdType type;
		public CallThriftThread(CmdType type, CmdResultCodePack pack, int server_id, String SQL) {
			this.type = type;
			this.SQL = SQL;
			this.pack = pack;
			this.server_id = server_id;
		}
		public void run() {
			// 调用Region Server端的Thrift接口
			String SERVER_IP = master.getRegionServers().get(server_id).getIpAddress();
    		int SERVER_PORT = master.getRegionServers().get(server_id).getPort();
			TTransport transport = null;
			String resultString = null;
			try {
                transport = new TSocket(SERVER_IP, SERVER_PORT);
                TProtocol protocol = new TBinaryProtocol(transport);
                transport.open();
                SQLService.Client client = new SQLService.Client(protocol);
                switch(type) {
                    case SELECT:System.out.println(resultString = client.select(SQL) + " on server " + server_id);break;
                    case CREATE_TABLE: System.out.println(resultString = client.create(SQL) + " on server " + server_id); break;
                    case DROP_TABLE: System.out.println(resultString = client.drop(SQL) + " on server " + server_id); break;
                    case INSERT: System.out.println(resultString = client.insert(SQL) + " on server " + server_id); break;
                    case DELETE: System.out.println(resultString = client.dilete(SQL) + " on server " + server_id); break;
				default:
					System.out.println("No such instruction:"+type);
					break;
                }
            } catch (TTransportException e) {
                e.printStackTrace();
            } catch (TException e) {
                e.printStackTrace();
            } finally {
                if (null != transport) {
                    transport.close();
                }
            }
            if(resultString.contains("[ERROR]"))
            	pack.setResult(CmdResultCode.UnknownError);
            else
            	pack.setResult(CmdResultCode.Success);
		}
	}
}
