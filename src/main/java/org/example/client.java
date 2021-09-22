package org.example;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.example.pojo.CmdResultCode;
import org.example.pojo.CmdType;
import org.example.pojo.Region;
import org.example.pojo.Region.Duplicate;
import org.example.pojo.RegionServerMetadata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;

class table {
    List<RegionServerMetadata> RServers = new ArrayList();
    private String name; //����

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void addServer(RegionServerMetadata rs) {
        RServers.add(rs);
    }

    public void print() {
        int i = 0;
        System.out.print(name + ":");
        for (RegionServerMetadata temp : RServers) {
            System.out.print(++i + " : ");
            System.out.print(temp.getIpAddress());
            System.out.print(":");
            System.out.println(temp.getPort());
        }
        return;
    }
}

class cache {
    String address;
    List<table> data = new ArrayList<table>();
    public Map<String, Integer> accesstime = new HashMap<String, Integer>();

    public void cout() { //�鿴cache�ڵ�ȫ����Ϣ
        for (table temp : data) {
            temp.print();
        }
        for (Map.Entry<String, Integer> entry : accesstime.entrySet()) {
            String mapKey = entry.getKey();
            int mapValue = entry.getValue();
            System.out.println(mapKey + " : " + mapValue);
        }
    }

    public Boolean find(String tableName) {
        for (table temp : data) {
            if (temp.getName().equals(tableName)) return true;
        }
        return false;
    }

    public table get(String tableName) {
        for (table temp : data) {
            if (temp.getName().equals(tableName)) return temp;
        }
        return null;
    }

    public void clearTable() {
        data.clear();
    }

    public table gettable(String tablename) {
        for (table temp : data) {
            if (temp.getName().equals(tablename)) return temp;
        }
        return null;
    }
}

public class client {
    String serverNode = "/serverParent";
    String regionNode = "/regionParent";
    String zkAddr = "127.0.0.1:2181";
    final String masterIp = "127.0.0.1";
    final int masterPort = 8099;
    public static final int TIMEOUT = 30000;

    private CuratorFramework zkClient;
    private cache client_cache = new cache();

    public void startClient(String SQL, String ip, int port) {
        TTransport transport = null;
        try {
            transport = new TSocket(ip, port);
            TProtocol protocol = new TBinaryProtocol(transport);
            transport.open();
            SQLService.Client client = new SQLService.Client(protocol);
            System.out.printf("Querying RegionServer %s:%d...\n", ip, port);
            System.out.println(client.select(SQL));
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            if (null != transport) {
                transport.close();
            }
        }
    }

    public CmdResultCode Client2Master(String SQL, CmdType type, String TableName)
    {
        TTransport transport = null;
        try {
            transport = new TSocket(masterIp, masterPort);
            TProtocol protocol = new TBinaryProtocol(transport);
            transport.open();
            Client2MasterService.Client client = new Client2MasterService.Client(protocol);

            return client.Client2Master(SQL, type, TableName); // FIXME
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            if (null != transport) {
                transport.close();
            }
        }
        return CmdResultCode.UnknownError;
    }

    public CmdResultCode sql_extract(CmdType type, String sql) {
        CmdResultCode res = CmdResultCode.Success;
        String SQL = sql;
        String TableName = null;

        String[] arr1 = sql.split(" ");

        if (type == CmdType.CREATE_TABLE) {
            for (int i = 1; i < arr1.length; i++) {
                if (arr1[i - 1].equals("table")) TableName = arr1[i];
            }
        } else if (type == CmdType.CREATE_INDEX) {
            for (int i = 1; i < arr1.length; i++) {
                if (arr1[i - 1].equals("on")) TableName = arr1[i];
            }
        } else if (type == CmdType.DROP_TABLE) {
            for (int i = 1; i < arr1.length; i++) {
                if (arr1[i - 1].equals("table")) TableName = arr1[i];
            }
        } else if (type == CmdType.DROP_INDEX) {
            for (int i = 1; i < arr1.length; i++) {
                if (arr1[i - 1].equals("on")) TableName = arr1[i];
            }
        } else if (type == CmdType.INSERT) {
            for (int i = 1; i < arr1.length; i++) {
                if (arr1[i - 1].equals("into")) TableName = arr1[i];
            }
        } else if (type == CmdType.DELETE) {
            for (int i = 1; i < arr1.length; i++) {
                if (arr1[i - 1].equals("from")) TableName = arr1[i];
            }
        }

        res = Client2Master(SQL, type, TableName);

        if(res != CmdResultCode.Success) {
            System.out.printf("[sql_extract] %s", res);
        }

        return res;
    }

    public CmdType judge_sql(String query) { //�����ж�������﷨�Ƿ���ȷ,���ж�����
        if (query.indexOf("join") != -1) return CmdType.Nofunc;

        if (query.indexOf("select") != -1) return CmdType.SELECT;

        if (query.indexOf("create table") != -1) return CmdType.CREATE_TABLE;
        if (query.indexOf("drop table") != -1) return CmdType.DROP_TABLE;

        if (query.indexOf("create index") != -1) return CmdType.CREATE_INDEX;
        if (query.indexOf("drop index") != -1) return CmdType.DROP_INDEX;

        if (query.indexOf("insert") != -1) return CmdType.INSERT;

        if (query.indexOf("delete") != -1) return CmdType.DELETE;

        return CmdType.SyntaxError;
    }

    public void get_Address(String sql, String address, int port) {
        if (client_cache.accesstime.get(address) == null) {
            client_cache.accesstime.put(address, 1);
        } else {
            int temp = client_cache.accesstime.get(address);
            temp = temp + 1;
            client_cache.accesstime.put(address, temp);
        }

        startClient(sql, address, port);
    }

    public String table_extract(String query) {  //��Select����л�ñ���

        String[] arr1 = query.split(" ");
        for (int i = 1; i < arr1.length; i++) {
            if (arr1[i - 1].equals("from")) return arr1[i];
        }

        return "ERROR";
    }

    RegionServerMetadata parseRegionServer(String serverInfo) {
        String[] splitInfo = serverInfo.split("\\|");
        int id = Integer.parseInt(splitInfo[0]);
        String ipAddress = splitInfo[1];
        int port = Integer.parseInt(splitInfo[2]);
        return new RegionServerMetadata(id, ipAddress, port);
    }

    public void cache_update() {
        try {
            cache tmpCache = new cache();
//            client_cache.clearTable();
            List<String> allRegions = zkClient.getChildren().forPath(regionNode);
//            System.out.println("[cache_update] allRegions=" + allRegions);
            for (String region : allRegions) {
                Region reg = new Region(new String(zkClient.getData().forPath(regionNode + "/" + region)));
                table tt = new table();
                tt.setName(reg.getTableName());
                ArrayList<Duplicate> duplicates = reg.getDuplicates();
                for (int i = 0; i < duplicates.size(); i++) {
                    if(!duplicates.get(i).getValid()) {
                        continue;
                    }
                    byte[] serverBytes = zkClient.getData().forPath(serverNode + "/server" + duplicates.get(i).getServer_id());
                    String serverInfo = new String(serverBytes);
                    RegionServerMetadata rs = parseRegionServer(serverInfo);
                    tt.addServer(rs);
                }
                tmpCache.data.add(tt);
            }
            client_cache = tmpCache;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return;
    }

    public void ava_search(table TableName, String sql) {
        try {
            int max = TableName.RServers.size(), min = 0;
            int ran2 = (int) (Math.random() * (max - min) + min);
            get_Address(sql, TableName.RServers.get(ran2).getIpAddress(), TableName.RServers.get(ran2).getPort());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return;
    }

    public void exec(String Filename) {
        File file = new File(Filename);
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
            String line;
            while ((line = br.readLine()) != null) {
                System.out.printf("DmSQL> %s\n", line);
                if(line.charAt(0) == '-') {
                    continue;
                }
                Thread thread = new Thread(new client_action(line));
                thread.start();
                thread.join(); // FIXME: ,,,
                Thread.sleep(10); // TODO: delete me; it works
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return;
    }

    public client() {
        System.out.println("[client] Welcome to our Distributed MiniSQL. Now, Client is started");
        try {
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
            zkClient = CuratorFrameworkFactory.builder()
                    .connectString(zkAddr)
                    .sessionTimeoutMs(5000)
                    .connectionTimeoutMs(5000)
                    .retryPolicy(retryPolicy)
                    .build();
            zkClient.start();
            cache_update();

            PathChildrenCache pathChildrenCache = new PathChildrenCache(zkClient, regionNode, true);
            pathChildrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
            pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
                public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event) throws Exception {
                cache_update();
                }
            });
//            pathChildrenCache.start(); // FIXME: java.lang.IllegalStateException: already started

            System.out.println("[client] Connect to ZooKeeper successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }

        Scanner in = new Scanner(System.in);
        System.out.printf("DmSQL> ");
        while(in.hasNext()) {
            String line = in.nextLine();
            String[] lineSplit = line.split(" ");
            if("exec".equals(lineSplit[0])) {
                exec(lineSplit[1]);
            } else {
                Thread thread = new Thread(new client_action(line));
                thread.start();
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.printf("DmSQL> ");
        }
    }

    class client_action implements Runnable {
        CmdType type;
        String sql;

        public client_action(String SQL) {
            try {
                this.type = judge_sql(SQL);
                int spaceIdx = 0;
                while (SQL.charAt(spaceIdx) != ' ') {
                    spaceIdx++;
                }
                spaceIdx++;
                this.sql = SQL.substring(spaceIdx);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void run() {
            if (type == CmdType.SELECT) {
                table this_table = client_cache.gettable(table_extract(sql)); // FIXME: this_table is null
                if (this_table == null) {
                    System.out.println("The table you queried doesn't exist.");
                } else {
                    ava_search(this_table, sql);
                }
            } else if (type == CmdType.Nofunc) System.out.println("�ù�����δʵ��");
            else if (type == CmdType.SyntaxError) System.out.println("[ERROR] Syntax Error.");
            else { // connect to master
                CmdResultCode res = sql_extract(type, sql);
                System.out.println(res);
                if(res == CmdResultCode.Success && (type == CmdType.CREATE_TABLE || type == CmdType.DROP_TABLE)) { // FIXME
                    cache_update();
                }
            }
        }
    }

    public static void main(String[] args) {
        client client = new client();
    }
}
