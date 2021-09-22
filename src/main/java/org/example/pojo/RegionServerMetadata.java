package org.example.pojo;

public class RegionServerMetadata {
    private int id;
    private String ipAddress;
    private int port;

    public RegionServerMetadata(int id, String ipAddress, int port) {
        this.id = id;
        this.ipAddress = ipAddress;
        this.port = port;
    }

    public int getId(){
        return id;
    }

    public void setId(){
        this.id = id;
    }

    public String getIpAddress(){
        return ipAddress;
    }

    public void setIpAddress(String ipAddress){
        this.ipAddress = ipAddress;
    }

    public int getPort(){
        return port;
    }

    public void setPort(int port){
        this.port = port;
    }

    @Override
    public boolean equals(Object obj) {
        RegionServerMetadata rs = (RegionServerMetadata)obj;
        return this.id == rs.id && this.ipAddress == rs.ipAddress;
    }
}
