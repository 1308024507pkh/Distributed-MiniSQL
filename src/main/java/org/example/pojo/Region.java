package org.example.pojo;

import java.util.ArrayList;

public class Region {
	public class Duplicate{
		private int server_id;
		private boolean valid;
		public Duplicate(int server_id, boolean valid) {
			this.server_id = server_id;
			this.valid = valid;
		}
		public int getServer_id() {
			return server_id;
		}
		public void setServer_id(int server_id) {
			this.server_id = server_id;
		}
		public void setValid(boolean valid) {
			this.valid = valid;
		}
		public boolean getValid() {
			return valid;
		}
		
	}
    private int regionId;
    private String tableName;
    private ArrayList<Duplicate> duplicates;
    private boolean valid;

    public Region(int regionId, String tableName) {
        this.regionId = regionId;
        this.tableName = tableName;
        this.duplicates = new ArrayList<Duplicate>();
    }

    public Region(String regionInfo) {
        String[] splitInfo = regionInfo.split(" ");
        this.regionId = Integer.parseInt(splitInfo[0]);
        this.tableName = splitInfo[1];
        this.duplicates = new ArrayList<Duplicate>();

        for (int i = 2; i+1 < splitInfo.length; i++) {
            boolean validness = Integer.parseInt(splitInfo[i + 1]) == 1;
            int serverId = Integer.parseInt(splitInfo[i]);
            if(validness) {
                addDuplicate(serverId);
            }
        }
    }

    public int getRegionId(){
        return regionId;
    }

    public void setRegionId(int regionId){
        this.regionId = regionId;
    }

    public String getTableName(){
        return tableName;
    }

    public void setTableName(String tableName){
        this.tableName = tableName;
    }

    public ArrayList<Duplicate> getDuplicates(){
        return duplicates;
    }

    public void setDuplicates(ArrayList<Duplicate> duplicates){
        this.duplicates = new ArrayList<Duplicate>();
    	for(int i=0; i<duplicates.size(); i++){
            this.duplicates.add(duplicates.get(i));
        }
    }

    public void setValid(boolean valid){
        this.valid = valid;
    }

    public boolean getValid(){
        return valid;
    }

    public void addDuplicate(int serverId){
        duplicates.add(new Duplicate(serverId, true));
    }
    public void addDuplicate(int serverId, boolean valid) { duplicates.add(new Duplicate(serverId, valid)); }

    public void removeDuplicate(int serverId){
    	for(int j=duplicates.size()-1; j>=0;j--) {
    		if(duplicates.get(j).getServer_id() == serverId){
                duplicates.remove(j);
            }
    	}
    }

    public void setDuplicateValid(int serverId, boolean valid) {
        for(Duplicate duplicate:duplicates){
            if(duplicate.getServer_id() == serverId){
                duplicate.setValid(valid);
            }
        }
    }
}


