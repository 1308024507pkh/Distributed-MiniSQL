package org.example.service;

import org.example.service.SQLService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFramedTransport;

public class SQLImpl implements SQLService.Iface {
    final String SERVER_IP = "127.0.0.1";
    int port = 8848;
    TTransport transport = null;
    SQLService.Client client;

    public SQLImpl(int port) {
        this.port = port;
    }

    @Override
    public String select(String SQL){
        try {
            transport = new TSocket(SERVER_IP, port);
            // 协议要和服务端一致
            TProtocol protocol = new TBinaryProtocol(transport);
            transport.open();
            client = new SQLService.Client(protocol);
            System.out.println("[select]" + SQL);
            return client.select(SQL);
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            if (null != transport) {
                transport.close();
            }
        }
        return "[ERROR]select";
    }

    @Override
    public String create(String SQL) {
        try {
            transport = new TSocket(SERVER_IP, port);
            // 协议要和服务端一致
            TProtocol protocol = new TBinaryProtocol(transport);
            transport.open();
            client = new SQLService.Client(protocol);
            return client.create(SQL);
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            if (null != transport) {
                transport.close();
            }
        }
        return "[ERROR]create";
    }

    @Override
    public String drop(String SQL) {
        try {
            transport = new TSocket(SERVER_IP, port);
            // 协议要和服务端一致
            TProtocol protocol = new TBinaryProtocol(transport);
            transport.open();
            client = new SQLService.Client(protocol);
            return client.drop(SQL);
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            if (null != transport) {
                transport.close();
            }
        }
        return "[ERROR]drop";
    }

    @Override
    public String insert(String SQL) {
        try {
            transport = new TSocket(SERVER_IP, port);
            // 协议要和服务端一致
            TProtocol protocol = new TBinaryProtocol(transport);
            transport.open();
            client = new SQLService.Client(protocol);
            return client.insert(SQL);
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            if (null != transport) {
                transport.close();
            }
        }
        return "[ERROR]insert";
    }

    @Override
    public String dilete(String SQL) {
        try {
            transport = new TSocket(SERVER_IP, port);
            // 协议要和服务端一致
            TProtocol protocol = new TBinaryProtocol(transport);
            transport.open();
            client = new SQLService.Client(protocol);
            return client.dilete(SQL);
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            if (null != transport) {
                transport.close();
            }
        }
        return "[ERROR]dilete";
    }
}
