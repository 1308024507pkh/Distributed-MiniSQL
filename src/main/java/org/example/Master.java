package org.example;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.example.pojo.*;
import org.example.pojo.Region.Duplicate;

import java.util.*;

public class Master implements Watcher {
    String serverNode = "/serverParent";
    String regionNode = "/regionParent";
    String regionIdNode = "/nextRegionId";
    String zkAddr = "127.0.0.1:2181";
    static int nextRegionId = 1;
    private CuratorFramework zkClient;
    private QueryForwarder qf = new QueryForwarder();
    private HashMap<Integer, RegionServerMetadata> regionServers = new HashMap<Integer, RegionServerMetadata>();
    private HashMap<Integer, Region> regions = new HashMap<Integer, Region>();
    private ArrayList<RegionStore> serverToRegion = new ArrayList<RegionStore>();
    private LogManager LM;

    public HashMap<Integer, RegionServerMetadata> getRegionServers() {
        return regionServers;
    }

    public HashMap<Integer, Region> getRegions() {
        return regions;
    }

    public Master() {
        System.out.println("Welcome to our Distributed MiniSQL. Now, Master is started");
        try {
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
            zkClient = CuratorFrameworkFactory.builder()
                    .connectString(zkAddr)
                    .sessionTimeoutMs(5000)  // 会话超时时间
                    .connectionTimeoutMs(5000) // 连接超时时间
                    .retryPolicy(retryPolicy)
                    .build();
            zkClient.start();
            if (null == zkClient.checkExists().forPath(serverNode))
                zkClient.create().withMode(CreateMode.PERSISTENT).forPath(serverNode);
            if (null == zkClient.checkExists().forPath(regionNode))
                zkClient.create().withMode(CreateMode.PERSISTENT).forPath(regionNode);
            if (null == zkClient.checkExists().forPath(regionIdNode))
                zkClient.create().withMode(CreateMode.PERSISTENT).forPath(regionIdNode, String.valueOf(0).getBytes()); // FIXME: ,,,
//            System.out.println("[Master] Connect to ZooKeeper successfully.");

            initServers();
            initRegions();
            initNextRegionId();
            LM = new LogManager(this);
            qf.setMaster(this);
            startServer();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public class getsql implements Client2MasterService.Iface {
        @Override
        public CmdResultCode Client2Master(String SQL, CmdType type, String TableName) throws TException {
            switch (type) {
                case CREATE_TABLE: {
                    return qf.CREATE_TABLE(type, SQL, TableName);
                }
                case DROP_TABLE: {
                    return qf.DROP_TABLE(type, SQL, TableName);
                }
                case CREATE_INDEX: {
                    return qf.CREATE_INDEX(type, SQL, TableName);
                }
                case DROP_INDEX: {
                    return qf.DROP_INDEX(type, SQL, TableName);
                }
                case INSERT: {
                    return qf.INSERT(type, SQL, TableName);
                }
                case DELETE: {
                    return qf.DELETE(type, SQL, TableName);
                }
                default:
                    return CmdResultCode.InvalidCmdType;
            }
        }
    }

    public void startServer() {
        try {
//            System.out.println("Master TSimpleServer start ....");

            //在这里调用了 HelloWorldImpl 规定了接受的方法和返回的参数
            TProcessor tprocessor = new Client2MasterService.Processor<Client2MasterService.Iface>(new getsql());

            TServerSocket serverTransport = new TServerSocket(8099);
            TServer.Args tArgs = new TServer.Args(serverTransport);
            tArgs.processor(tprocessor);
            tArgs.protocolFactory(new TBinaryProtocol.Factory());

            TServer server = new TSimpleServer(tArgs);
            server.serve();

        } catch (Exception e) {
            System.out.println("Server start error!!!");
            e.printStackTrace();
        }
    }

    public QueryForwarder getQf() {
        return qf;
    }

    public LogManager getLM() { return LM; }

    public void initServers() {
        try {
            //读取所有的服务器
            List<String> allServers = zkClient.getChildren().usingWatcher(this).forPath(serverNode);
            for (String server : allServers) {
                String info = new String(zkClient.getData().forPath(serverNode + "/" + server));
                String[] splitInfo = info.split("\\|");
                regionServers.put(Integer.parseInt(splitInfo[0]), parseRegionServer(info));
                RegionStore regionStore = new RegionStore(Integer.parseInt(splitInfo[0]));
                serverToRegion.add(regionStore);
            }
            serverSort();
            printRegionServers();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void printRegionServers() {
        System.out.println("[initServers] There are " + regionServers.size() + " regionServers currently online:");
        for (RegionServerMetadata regionServerMetadata : regionServers.values()) {
            System.out.printf("\tserverId=%d, at %s:%d\n", regionServerMetadata.getId(), regionServerMetadata.getIpAddress(), regionServerMetadata.getPort());
        }
    }

    public void initRegions() {
        try {
            //读取所有的表
            List<String> allRegions = zkClient.getChildren().forPath(regionNode);
            for (String region : allRegions) {
                String info = new String(zkClient.getData().forPath(regionNode + "/" + region));
                String[] splitInfo = info.split(" ");
                System.out.println("[initRegions]" + info);
                Region r = new Region(info);
                Region r_new = new Region(r.getRegionId(), r.getTableName());
                ArrayList<Region.Duplicate> duplicates = r.getDuplicates();
                int flag = 0;
                for (int i = 0; i < duplicates.size(); i++) {
                    int j;
                    for (j = 0; j < serverToRegion.size(); j++) {
                        if (serverToRegion.get(j).getServerId() == duplicates.get(i).getServer_id()) {
                            break;
                        }
                    }
                    if (j == serverToRegion.size()){
                        flag = 1;
                    }
                    else{
                        r_new.addDuplicate(duplicates.get(i).getServer_id(), duplicates.get(i).getValid());
                        if (duplicates.get(i).getValid() != false)
                            serverToRegion.get(j).addRegionId(r.getRegionId());
                    }
                }
                if(flag == 1){
                    String content = new String("");
                    content += r_new.getRegionId();
                    content += " ";
                    content += r_new.getTableName();
                    ArrayList<Region.Duplicate> dups = r_new.getDuplicates();
                    for (int i = 0; i < dups.size(); i++) {
                        content += " ";
                        content += dups.get(i).getServer_id();
                        content += " ";
                        if (dups.get(i).getValid() == true) content += "1";
                        else content += "0";
                    }
                    zkClient.setData().forPath(regionNode + "/" + region, content.getBytes());
                }
                regions.put(Integer.parseInt(splitInfo[0]), r_new);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void initNextRegionId() {
        try {
            //读取下一个表的ID
            byte[] bytes = zkClient.getData().forPath(regionIdNode);
            nextRegionId = Integer.parseInt(new String(bytes));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void process(WatchedEvent event) {
        try {
//            Thread.sleep(200);
            List<String> allServers = zkClient.getChildren().usingWatcher(this).forPath(serverNode);
            HashMap<Integer, RegionServerMetadata> newRegionServers = new HashMap<Integer, RegionServerMetadata>();
            for (String server : allServers) {
                byte[] bytes = zkClient.getData().forPath(serverNode + "/" + server);
                String info = new String(bytes);
//                System.out.println("[process] " + server + ";" + info);
                String[] splitInfo = (new String(bytes)).split("\\|");

                newRegionServers.put(Integer.parseInt(splitInfo[0]), parseRegionServer(info));
            }
            if (newRegionServers.size() > regionServers.size()) {
                for (int serverId : newRegionServers.keySet()) {
                    if (!regionServers.containsKey(serverId)) {
                        System.out.printf("A new RegionServer(serverId=%d, at %s:%d) has been added.\n", serverId, newRegionServers.get(serverId).getIpAddress(), newRegionServers.get(serverId).getPort());
                        addServer(newRegionServers.get(serverId));
                    }
                }
            } else {
                for (int serverId : regionServers.keySet()) {
                    if (!newRegionServers.containsKey(serverId)) {
                        System.out.printf("An existing RegionServer(serverId=%d, at %s:%d) has been removed.\n", serverId, regionServers.get(serverId).getIpAddress(), regionServers.get(serverId).getPort());
                        deleteServer(regionServers.get(serverId));
                    }
                }
            }
            regionServers = newRegionServers;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    RegionServerMetadata parseRegionServer(String serverInfo) {
        String[] splitInfo = serverInfo.split("\\|");
        int id = Integer.parseInt(splitInfo[0]);
        String ipAddress = splitInfo[1];
        int port = Integer.parseInt(splitInfo[2]);
        return new RegionServerMetadata(id, ipAddress, port);
    }

    public LinkedList<RegionServerMetadata> findNewServer() {
        LinkedList<RegionServerMetadata> relatedServers = new LinkedList<RegionServerMetadata>();
        synchronized (serverToRegion) {
            if (serverToRegion.size() == 0) { // FIXME
                System.out.println("[findNewServer] Cant find new server");
            } else if (serverToRegion.size() == 1) {
                int serverId = serverToRegion.get(0).getServerId();
                relatedServers.add(regionServers.get(serverId));
            } else if (serverToRegion.size() > 1) {
                int serverId1 = serverToRegion.get(0).getServerId();
                int serverId2 = serverToRegion.get(1).getServerId();
                relatedServers.add(regionServers.get(serverId1));
                relatedServers.add(regionServers.get(serverId2));
            }
            return relatedServers;
        }
    }

    public void addRegion(String tableName, LinkedList<Integer> server_id_list, LinkedList<RegionServerMetadata> rs_list, boolean redo) {
        try {
            Region region = null;
            if(redo) {
                for(Region tmpRegion: regions.values()) {
                    if(tmpRegion.getTableName().equals(tableName)) {
                        region = tmpRegion;
                        break;
                    }
                }
            }else{
                region = new Region(nextRegionId++, tableName);
                zkClient.setData().forPath(regionIdNode, String.valueOf(nextRegionId).getBytes());
            }
            synchronized (regions) {
                synchronized (serverToRegion) {
                    for (int server : server_id_list) {
                        for (int i = 0; i < serverToRegion.size(); i++) {
                            if (serverToRegion.get(i).getServerId() == server) {
                                if(redo) {
                                    if(!serverToRegion.get(i).getRegionIds().contains(region.getRegionId())) {
                                        serverToRegion.get(i).addRegionId(region.getRegionId());
                                    }
                                }else {
                                    serverToRegion.get(i).addRegionId(region.getRegionId());
                                }
                                break;
                            }
                        }
                    }
                }
                serverSort();
                for (RegionServerMetadata server : rs_list) {
                    if (server_id_list.contains(server.getId())) {
                        if (redo) {
                            region.setDuplicateValid(server.getId(), true);
                        } else {
                            region.addDuplicate(server.getId(), true);
                        }
                    } else {
                        if (!redo) {
                            region.addDuplicate(server.getId(), false);
                        }
                    }
                }
                regions.put(region.getRegionId(), region);
            }

            String content = new String("");
            content += region.getRegionId();
            content += " ";
            content += region.getTableName();
            ArrayList<Region.Duplicate> duplicates = region.getDuplicates();
            for (int i = 0; i < duplicates.size(); i++) {
                content += " ";
                content += duplicates.get(i).getServer_id();
                content += " ";
                if (duplicates.get(i).getValid() == true) content += "1";
                else content += "0";
            }
//            System.out.println("[addRegion] " + (regionNode + "/" + tableName) + ";" + content);
            System.out.println("[addRegion] serverToRegion=");
            synchronized (serverToRegion) {
                for (RegionStore rStore : serverToRegion) {
                    System.out.printf("\t%d -> ", rStore.getServerId());
                    for (Integer regionId : rStore.getRegionIds()) {
                        System.out.printf("%s,", regions.get(regionId).getTableName());
                    }
                    System.out.println();
                }
            }
            if (null == zkClient.checkExists().forPath(regionNode+ "/" + tableName)) {
                zkClient.create().withMode(CreateMode.PERSISTENT).forPath(regionNode + "/" + tableName, content.getBytes());
            } else {
//                System.out.println("[addRegion] content = "+content);
                zkClient.setData().forPath(regionNode + "/" + tableName, content.getBytes());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void deleteRegion(String tableName, LinkedList<Integer> server_id_list, LinkedList<RegionServerMetadata> rs_list) {
        try {
            int flag = 0;
            for (RegionServerMetadata server : rs_list) {
                if (!server_id_list.contains(server.getId())) {
                    flag = 1;
                    break;
                }
            }
            synchronized (regions) {
                LinkedList<Integer> keys = new LinkedList<Integer>(regions.keySet());
                for (int k=keys.size()-1; k>=0; k--) {
                    Region region = regions.get(keys.get(k));
                    if (region.getTableName().equals(tableName)) {
                        ArrayList<Duplicate> duplicates = region.getDuplicates();
                        synchronized (serverToRegion) {
                            for (int i = 0; i < duplicates.size(); i++) {
                                int serverId = duplicates.get(i).getServer_id();

                                for (int j = serverToRegion.size()-1; j >=0 ; j--) {
                                    if (serverToRegion.get(j).getServerId() == serverId) {
                                        serverToRegion.get(j).removeRegionId(region.getRegionId());
                                    }
                                }
                            }
                        }
                        serverSort();

                        if (flag == 0) {
                            regions.remove(new Integer(region.getRegionId()));
                            zkClient.delete().forPath(regionNode + "/" + tableName);
                        } else {
                            for (RegionServerMetadata server : rs_list) {
                                if (server_id_list.contains(server.getId())) {
                                    regions.get(region.getRegionId()).removeDuplicate(server.getId());
                                } else {
                                    regions.get(region.getRegionId()).setDuplicateValid(server.getId(), false);
                                }
                            }
                            String content = new String("");
                            content += region.getRegionId();
                            content += " ";
                            content += region.getTableName();
                            duplicates = region.getDuplicates();
                            for (int i = 0; i < duplicates.size(); i++) {
                                content += " ";
                                content += duplicates.get(i).getServer_id();
                                content += " ";
                                if (duplicates.get(i).getValid() == true) content += "1";
                                else content += "0";
                            }
                            zkClient.setData().forPath(regionNode + "/" + tableName, content.getBytes());
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void addServer(RegionServerMetadata rs) {
//        System.out.println("[addServer] entry");
        try { // FIXME
//            if (serverToRegion.size() == 1) {  //serverToRegion.size() == 1
//                ArrayList<Integer> regionLists = serverToRegion.get(0).getRegionIds();
//                RegionStore regionStore = new RegionStore(rs.getId());
//                regionStore.setRegionIds(regionLists);
//                serverToRegion.add(regionStore);
//
//                for (Region region : regions.values()) {
//                    region.addDuplicate(rs.getId());
//                    String tableName = region.getTableName();
//                    byte[] bytes = zkClient.getData().forPath(regionNode + "/" + tableName);
//                    String info = new String(bytes);
//                    info += " ";
//                    info += rs.getId();
//                    zkClient.setData().forPath(regionNode + "/" + tableName, info.getBytes());
//                }
//            } else {
            {
                RegionStore regionStore = new RegionStore(rs.getId());
                synchronized (serverToRegion) {
                    serverToRegion.add(regionStore);
                }
//                System.out.println("[addServer] serverToRegion updated");
            }
            serverSort();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deleteServer(RegionServerMetadata rs) {
        try {
            int id = rs.getId();
            ArrayList<Integer> regionLists = new ArrayList<Integer>();
            synchronized (serverToRegion) {
                for (int i = 0; i < serverToRegion.size(); i++) {
                    if (serverToRegion.get(i).getServerId() == id) {
                        regionLists = serverToRegion.get(i).getRegionIds();
                        serverToRegion.remove(i);
                    }
                }
            }
            if (serverToRegion.size() == 0) {
                regions.clear();
                List<String> allRegions = zkClient.getChildren().forPath(regionNode);
                for (String region : allRegions) {
                    zkClient.delete().forPath(regionNode + "/" + region);
                }
            } else {
                synchronized (serverToRegion) {
                    for (int i = 0; i < regionLists.size(); i++) {  //对于每一个表
                        // 删除表中被删除的server
                        Region region = regions.get(regionLists.get(i));
                        String tableName = region.getTableName();
                        region.removeDuplicate(id);
                        byte[] bytes = zkClient.getData().forPath(regionNode + "/" + tableName);
                        String info = new String(bytes);
//                        System.out.printf("[deleteServer] info=%s\n", info);
                        String[] splitInfo = info.split(" ");
                        int j = 2;
                        for (; j < splitInfo.length; j+=2) {
                            if (Integer.parseInt(splitInfo[j]) == id) break;
                        }
                        info = splitInfo[0];
                        for (int k = 1; k < splitInfo.length; k++) {
                            if (k != j && k !=j+1) {
                                info += " ";
                                info += splitInfo[k];
                            }
                        }
//                        System.out.printf("[deleteServer] info=%s\n", info);
                        // 表的重分配
                        for (j = 0; j < serverToRegion.size(); j++) {
                            if (serverToRegion.get(j).containRegion(regionLists.get(i)) == false) {
                                serverToRegion.get(j).addRegionId(regionLists.get(i));
                                int serverId = serverToRegion.get(j).getServerId();
                                regions.get(regionLists.get(i)).addDuplicate(serverId, false);
                                info += " ";
                                info += serverId;
                                info += " 0";
                                break;
                            }
                        }
//                        System.out.printf("[deleteServer] info=%s\n", info);
                        zkClient.setData().forPath(regionNode + "/" + tableName, info.getBytes());
                    }
                }
                serverSort();
                LM.Crash();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void serverSort() {
        synchronized (serverToRegion) {
            Collections.sort(serverToRegion, new Comparator<RegionStore>() {
                public int compare(RegionStore rs1, RegionStore rs2) {
                    if (rs1.getRegionIds().size() == rs2.getRegionIds().size())
                        return (new Integer(rs1.getServerId())).compareTo(rs2.getServerId());
                    else
                        return (new Integer(rs1.getRegionIds().size())).compareTo(rs2.getRegionIds().size());
                }
            });
        }
    }

    public void addLogRecord(LogRecord lr) {
        LM.addRecord(lr);
    }

    public static void main(String[] args) {
        Master master = new Master();
        for (; ; ) ;
    }

}
