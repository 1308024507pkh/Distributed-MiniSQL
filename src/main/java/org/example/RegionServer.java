package org.example;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import org.example.service.SQLService;
import org.example.service.SQLImpl;

import java.nio.charset.StandardCharsets;

public class RegionServer {
    String serverNode = "/serverParent";
    String zkAddr = "127.0.0.1:2181";
    private CuratorFramework zkClient;
    private static int serverId = 1;
    public static int port = 8090;
    private static int miniSQLPort = 8848;
    private static final String ip = "192.168.43.87"; // TODO: to update

    public RegionServer() {
        final String serverInfo = serverId+"|"+ip+"|"+port;
        try {
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
            zkClient = CuratorFrameworkFactory.builder()
                    .connectString(zkAddr)
                    .sessionTimeoutMs(5000)  // 会话超时时间
                    .connectionTimeoutMs(5000) // 连接超时时间
                    .retryPolicy(retryPolicy)
                    .build();
            zkClient.start();
//            System.out.printf("[RegionServer] serverId=%d\n", serverId);
            if(null == zkClient.checkExists().forPath(serverNode)) {
                zkClient.create().withMode(CreateMode.PERSISTENT).forPath(serverNode);
            }
            if(null == zkClient.checkExists().forPath(serverNode+"/server"+serverId))
                zkClient.create().withMode(CreateMode.EPHEMERAL).forPath(serverNode+"/server"+serverId, serverInfo.getBytes());
        } catch(Exception e){
            e.printStackTrace();
        }
    }

    public void startServer() {
        try {
            System.out.printf("RegionServer %d start at %s:%d, miniSQL should have started at %s:%d\n", serverId, ip, port, ip, miniSQLPort);

            //在这里调用了 HelloWorldImpl 规定了接受的方法和返回的参数
            TProcessor tprocessor = new SQLService.Processor<SQLService.Iface>( new SQLImpl(miniSQLPort));

            TServerSocket serverTransport = new TServerSocket(port);
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

    public static void main(String[] args) {
        System.out.println("RegionServer <port=8090> <miniSQLPort=8848>");
        try {
            if(args.length >= 1) {
                port = Integer.parseInt(args[0]);
            }
            if(args.length >= 2) {
                miniSQLPort = Integer.parseInt(args[1]);
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
        serverId = (ip+port).hashCode();

        RegionServer server = new RegionServer();
        server.startServer();
    }
}
