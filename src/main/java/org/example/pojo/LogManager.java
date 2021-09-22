package org.example.pojo;

import java.io.*;
import java.util.*;

import org.example.Master;

public class LogManager {
	Master master;
	private LinkedList<LogRecord> log_list;
	private int all_finished;
	Boolean crash;
	String logPath = System.getProperty("user.dir") + "\\log.txt";
	LogTinker l;
	public LogManager(Master master) {
		this.master = master;
		crash = false;
		log_list = new LinkedList<LogRecord>();
		Scanner in=new Scanner(System.in);
		File f=new File(logPath);
		BufferedReader reader;
		// 读取log.txt的信息，生成日志列表
		try {
			reader = new BufferedReader(new FileReader(f));
			String temp=null;
			temp = reader.readLine();
			all_finished = Integer.valueOf(temp);
			while((temp=reader.readLine())!=null){
				String[] temps = temp.split("\\$");
				String[] server_list = temps[3].split("\\|");
				LinkedList<Integer> server_id_list = new LinkedList<Integer>();
				for(int i=0; i<server_list.length; i++) {
					server_id_list.add(Integer.valueOf(server_list[i]));
				}
				LogRecord log = new LogRecord(CmdType.valueOf(temps[0]), temps[2], temps[1], server_id_list);
				this.addRecord(log);
			}
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		// 启动日志修补线程
		l = new LogTinker();
		Thread t = new Thread(l);
		t.start(); // TODO
	}
	public void addRecord(LogRecord log) {
		synchronized (log_list) {
			log_list.add(log);
		}
	}
	public int getAll_finished() {
		return all_finished;
	}
	public String toString() {
		String s = "";
		for (int i = 0; i < log_list.size(); i++) {
			s += log_list.get(i).toString();
		}
		return s;
	}
	public void flush() {
		// 将日志内容写入磁盘
		synchronized (log_list){
			BufferedWriter bw = null;
			File f=new File(logPath);
			try {
				bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f), "UTF-8"));
				bw.write(all_finished + "");
				bw.newLine();
				bw.flush();
				for(int i=0; i<log_list.size(); i++) {
					bw.write(log_list.get(i).toString());
					bw.flush();
				}
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	class CheckResult{
		// 某条日志是否成功执行的结果类
		private boolean success;
		private Region region;
		public CheckResult(boolean success, Region region) {
			this.success = success;
			this.region = region;
		}
		public Region getRegion() {
			return region;
		}
		public void setRegion(Region region) {
			this.region = region;
		}
		public boolean isSuccess() {
			return success;
		}
		public void setSuccess(boolean success) {
			this.success = success;
		}
	}
	public void Crash() {
		// 出现宕机
		synchronized (crash) {
			System.out.println("[Crash]");
			crash = true;
		}
	}
	class LogTinker implements Runnable{
		@Override
		public void run() {
			scan:
			while(true) {
				try {
					synchronized (crash) {
						if(crash) {
							all_finished = -1;
							crash = false;
							continue scan;
						}
					}
					// 每1秒启动一次
					Thread.sleep(1000);
					LinkedList<RedoLogRecord> redo_list = new LinkedList<RedoLogRecord>();
					LinkedList<Integer> no_redo_list = new LinkedList<Integer>();
					// 从完成点开始，遍历日志列表，找到需要重做的日志
					for(int i=all_finished + 1; i<log_list.size(); i++) {
						// 发生宕机，终止重做，从头开始
						synchronized (crash) {
							if(crash) {
								all_finished = -1;
								crash = false;
								continue scan;
							}
						}
						LogRecord log = log_list.get(i);
						// 检测该日志记录是否需要重做
						CheckResult finished = checkfinished(log);
						// 如果该日志记录已经成功执行且为或DROP_TABLE或CREATE_TABLE型，则其前面所有的相同Region的日志都不会重做
						if(finished.isSuccess()&&(log.type==CmdType.DROP_TABLE||log.type==CmdType.CREATE_TABLE)) {
							for(int j=0;j<redo_list.size();j++) {
								if(log.TableName.equals(redo_list.get(j).TableName)) {
									if(!no_redo_list.contains(j)) {
										no_redo_list.add(j);
									}
								}
							}
							Collections.sort(no_redo_list);
						}
						// 
						else if(finished.isSuccess()) {
							continue;
						}
						// 需要重做，加入重做列表
						else {
							redo_list.add(new RedoLogRecord(log, i, finished.getRegion()));
						}
					}
					// 重做并刷新日志文件
					if(redo_list.size()>0){
						redo(redo_list, no_redo_list);
						flush();
					}else if(all_finished != log_list.size()-1) {
						all_finished = log_list.size()-1;
						flush();
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private CheckResult checkfinished(LogRecord lr) {
		String tableName = lr.TableName;
		LinkedList<Integer> server_id_list = lr.server_id_list;
		Region region = null;
		// 找到日志所在的Region
		for(int id : master.getRegions().keySet()){
			region = master.getRegions().get(id);
			if(region.getTableName().equals(tableName)) {
				break;
			}
			region = null;
		}
		// 未找到，不需要重做
		if (region == null){
			return new CheckResult(true, region);
		}
		// 找到
		switch (lr.type) {
			// DROP_TABLE，一定重做
			case DROP_TABLE:{
				return new CheckResult(false, region);
			}
			// 其余语句，若该region的副本所在的Region Server列表L1是日志记录中Region Server列表L2的子集，不需要重做
			case CREATE_TABLE:
			case CREATE_INDEX:
			case DROP_INDEX:
			case INSERT:
			case DELETE:{
				for(int i=0; i<region.getDuplicates().size(); i++) {
					boolean done = false;
					int server_id = region.getDuplicates().get(i).getServer_id();
					for(int j=0; j<server_id_list.size(); j++) {
						if(server_id==server_id_list.get(j)) {
							done = true;
							break;
						}
					}
					if(done==false) {
						return new CheckResult(false, region);
					}
				}
				return new CheckResult(true, region);
			}
			default:{
				return new CheckResult(true, region);
			}
		}
	}

	private void redo(LinkedList<RedoLogRecord> redo_list, LinkedList<Integer> no_redo_list) {
		for(int i=0; i<redo_list.size(); i++) {
			// 若宕机，终止重做，完成点置为初始值，从头重做
			synchronized (crash) {
				if(crash) {
					all_finished = -1;
					crash = false;
					return;
				}
			}
			RedoLogRecord lr = redo_list.get(i);
			// 如果该日记记录位于不重做列表，则continue
			if(no_redo_list.size()>0 && no_redo_list.getFirst()==i) {
				no_redo_list.removeFirst();
				if (i<redo_list.size()-1) {
					all_finished = redo_list.get(i+1).getId()-1;
				}
				else {
					all_finished++;
				}
				continue;
			}
			// 重做此日志
			CmdResultCode resultCode = redo_log(lr);
			// 若返回结果为成功，则更新完成点
			if(resultCode==CmdResultCode.Success) {
				System.out.println("[Redo Success] "+lr.SQL);
				if(all_finished+1==lr.getId()) {
					if (i<redo_list.size()-1) {
						all_finished = redo_list.get(i+1).getId()-1;
					}
					else {
						all_finished++;
					}
				}
			}else {
				System.out.println("[Redo Fail] [" + resultCode + "]" +lr.SQL);
			}
		}
	}

	private CmdResultCode redo_log(RedoLogRecord log) {
		LinkedList<Integer> server_id_list = new LinkedList<Integer>();
		for(int i=0;i<log.server_id_list.size();i++) {
			server_id_list.add(log.server_id_list.get(i));
		}
		// 获取该日志记录所在的region
		Region region = log.getRegion();
		CmdResultCode result = CmdResultCode.UnknownError;
		switch (log.type) {
			case CREATE_INDEX:
			case DROP_INDEX:
			case INSERT:
			case DELETE:{
				// 调用QueryForwarder的接口，将SQL转发给应重做的Region Server
				result = master.getQf().CallRegionServerInterface(log.type, region, server_id_list, log.SQL);
				// 若成功，更新日志
				if(result==CmdResultCode.Success) {
					log_list.get(log.getId()).server_id_list = server_id_list;
					//System.out.println(log_list.get(log.getId()));
				}
				break;
			}
			case CREATE_TABLE:{
				
				LinkedList<RegionServerMetadata> rs_list = new LinkedList<RegionServerMetadata>();
				for(int i=0; i<log.region.getDuplicates().size(); i++) {
					if(log.region.getDuplicates().get(i).getValid()==false) {
						int sid = log.region.getDuplicates().get(i).getServer_id();
						rs_list.add(master.getRegionServers().get(sid));
					}
				}
				// 调用QueryForwarder的接口，将SQL转发给应重做的Region Server
				// 若成功，更新分区信息和日志
				result = master.getQf().CallRegionServerInterface(log.type, rs_list, server_id_list, log.SQL);
				if(result==CmdResultCode.Success) {
					master.getQf().ChangeDistributionInfo(log.type, log.TableName, server_id_list, rs_list, true);
					log_list.get(log.getId()).server_id_list = server_id_list;
					//System.out.println(log_list.get(log.getId()));
				}
				break;
			}
			case DROP_TABLE:{
				server_id_list = log.server_id_list;
				// 调用QueryForwarder的接口，将SQL转发给应重做的Region Server
				// 若成功，更新分区信息和日志
				result = master.getQf().CallRegionServerInterface(log.type, region, server_id_list, log.SQL);
				if(result==CmdResultCode.Success) {
					LinkedList<RegionServerMetadata> rs_list = new LinkedList<RegionServerMetadata>();
					for(int i=0; i<region.getDuplicates().size(); i++) {
						rs_list.add(master.getRegionServers().get(region.getDuplicates().get(i).getServer_id()));
					}
					master.getQf().ChangeDistributionInfo(log.type, log.TableName, server_id_list, rs_list);
					log_list.get(log.getId()).server_id_list = server_id_list;
					//System.out.println(log_list.get(log.getId()));
				}

				break;
			}
			default:
				break;
		}
		return result;
	}
}

