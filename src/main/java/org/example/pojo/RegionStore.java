package org.example.pojo;

import java.util.ArrayList;

public class RegionStore {
    int serverId;
    ArrayList<Integer> regionIds;

    public RegionStore(int serverId){
        this.serverId = serverId;
        regionIds = new ArrayList<Integer>();
    }

    public void setServerId(int serverId){
        this.serverId = serverId;
    }

    public int getServerId(){
        return serverId;
    }

    public void setRegionIds(ArrayList<Integer> regionIds){
        for(int i=0; i<regionIds.size(); i++){
            this.regionIds.add(regionIds.get(i));
        }
    }

    public ArrayList<Integer> getRegionIds(){
        return regionIds;
    }

    public void addRegionId(int regionId){
        regionIds.add(regionId);
    }

    public void removeRegionId(int regionId){
        regionIds.remove(new Integer(regionId));
    }

    public boolean containRegion(int regionId){
        return regionIds.contains(new Integer(regionId));  //直接regionId?
    }
    /*
    @Override
    public int compareTo(RegionStore rs) {
        if(this.regionIds.size() == rs.regionIds.size())
            return (new Integer(this.serverId)).compareTo(rs.serverId);
        else
            return (new Integer(this.regionIds.size())).compareTo(rs.regionIds.size());
    }
    */

}
