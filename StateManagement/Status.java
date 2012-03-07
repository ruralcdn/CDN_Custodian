package StateManagement ;

import java.sql.*;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
public class Status{
	public String contentId;
	public int type;
	public int totseg;
	public int curseg;
	public int off ;
	public int prefint;
	public String prefrt;
	public String appid;
	public int sendmetadata;
	public String prefrtport;
	String[] st = new String[8];
	Connection con ;
    Statement stat ;
    PreparedStatement prep;
    private static Status myStatus;
	
    private   Status(){
		
		try{
			Class.forName("com.mysql.jdbc.Driver");
		}catch(ClassNotFoundException e){
			e.printStackTrace();
			
		}
						
		try {
			con = DriverManager.getConnection
				("jdbc:mysql://localhost:3306/ruralcdn","root","abc123");
		} catch (SQLException e) {
			e.printStackTrace();
		}
    }	
    public static synchronized Status getStatus(){
    	if(myStatus == null)
    		myStatus = new Status();
    	return myStatus;
    	
    }
	public void insertData(String table, String contentId, int type, int totseg, int curseg, int off, int prefint, String prefrt, String appid, int sendmetadata, String prefrtport ){
		
		try{
			prep = con.prepareStatement("insert into "+table+" values(?,?,?,?,?,?,?,?,?,?)");
			prep.setString(1,contentId);
			prep.setInt(2,type);
			prep.setInt(3,totseg);
			prep.setInt(4,curseg);
			prep.setInt(5,off);
			prep.setInt(6,prefint);
			prep.setString(7,prefrt);
			prep.setString(8,appid);
			prep.setInt(9,sendmetadata);
			prep.setString(10,prefrtport);
			prep.execute();
			
		}catch(Exception e){
			System.out.println("Exception occurs at insertData");
		}
	}
	public void insertData(String table, String data){
		try{
			prep = con.prepareStatement("insert into "+table+" values(?)");
			prep.setString(1,data);
			prep.execute();
			prep.close();
		}catch(Exception e){
			System.out.println("Exception occurs at insertData2");
		}
	}
	
	public void delData(String query){
		try{
			stat = con.createStatement();
			stat.execute(query);
			stat.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	public boolean execQuery(String s,String contentId, int type){
		boolean flag = false;
		ResultSet resset = null ;
		try{
			stat = con.createStatement();
			stat.execute(s);
			resset = stat.getResultSet();
			while(resset.next()){
				String str = resset.getString("contentid");
				int t = resset.getInt("type");
				if(str.equals(contentId) && t== type)
					{
						flag = true ;
						break ;
					}	
				}
			resset.close();
			stat.close();
		}catch(Exception e){
			System.out.println("Exception occurs at execQuery");
		}
		return flag ;
	}
	public String[] execQuery(String s){
		ResultSet resset = null ;
		try{
			stat = con.createStatement();
			stat.execute(s);
			resset = stat.getResultSet();
			st[0]="";
			st[1]="";
			st[2]="";
			st[3]="";
			st[4]="";
			st[5]="";
			st[6]="";
			st[7]="";
			while(resset.next()){
				st[0] = Integer.toString(resset.getInt("curseg")) ;
				st[1] = Integer.toString(resset.getInt("off") );
				st[2] = Integer.toString(resset.getInt("prefint") );
				st[3] = Integer.toString(resset.getInt("totseg")) ;
				st[4] = resset.getString("appid");
				st[5] = Integer.toString(resset.getInt("sendmetadata"));
				st[6] = resset.getString("prefrt");
				st[7] = resset.getString("prefrtport");
				 
			}	
			resset.close();
			stat.close();
		}catch(Exception e){
			System.out.println("Exception occurs at execQuery2");
		}
		return st ;
	}	
	
	public void updateData(String table){
	
		try{
					
			prep= con.prepareStatement("update "+table+" set totseg = ?, curseg = ?, off = ?," +
					" prefint = ?, prefrt = ?, appid = ?,sendmetadata = ?, prefrtport = ? where contentid = ? " +
					"and type = ?");
			prep.setInt(1,this.totseg);
			prep.setInt(2,this.curseg);
			prep.setInt(3,this.off);
			prep.setInt(4,this.prefint);
			prep.setString(5,this.prefrt);
			prep.setString(6,this.appid);
			prep.setInt(7,this.sendmetadata);
			prep.setString(8,this.prefrtport);
			prep.setString(9,this.contentId);
			prep.setInt(10,this.type);
			prep.execute();
			prep.close();
			

		}catch(Exception e){
			System.out.println("Exception occurs at updateData");
		}
	}	
	public void updateState(String table, String contentId, int type, int curseg){
		
		try{
					
			prep= con.prepareStatement("update "+table+" set curseg = ? where contentid = ? " +
					"and type = ?");
			prep.setInt(1,curseg);
			prep.setString(2,contentId);
			prep.setInt(3,type);
			prep.execute();
			prep.close();
			

		}catch(Exception e){
			System.out.println("Exception occurs at updateState");
		}
	}	
	
	public void updateState(String table, String contentId, int type){
		
		try{
			System.out.println("In updateStae for deleteing the status");		
			prep= con.prepareStatement("delete from "+table+" where contentid = ? " +
					"and type = ?");
			prep.setString(1,contentId);
			prep.setInt(2,type);
			prep.execute();
			prep.close();
			

		}catch(Exception e){
			System.out.println("Exception occurs at updateState");
		}
	}	
	
	public Map<String,String> setUploadRequests(String table, String contentId, String uploadId, String fileType){
		Map<String,String> uploadRequestList = new HashMap<String,String>();
		ResultSet resset = null ;
		try{
			
			prep = con.prepareStatement("insert into "+table+" values(?,?,?) ");
			prep.setString(1,contentId);
			prep.setString(2,uploadId);
			prep.setString(3,fileType);
			prep.execute();
			prep.close();
			stat = con.createStatement();
			stat.execute("select * from "+table);
		    resset = stat.getResultSet();
			while(resset.next()){
				uploadRequestList.put(resset.getString("contentid"),resset.getString("uploadId"));
			}
			resset.close();
			stat.close();
		}catch(Exception e){
			System.out.println("Exception occurs at setUploadrequests");
		}
		return uploadRequestList ;
	}
	
	public List<String> setDownloadRequests(String table, String contentId, int choice){
		List<String> downloadRequest = new ArrayList<String>();
		switch(choice)
		{
			case 0:
				try {
					stat = con.createStatement();
					stat.execute("select * from "+table+" where contentId ='"+contentId+"'");
					ResultSet rst = stat.getResultSet();
					if(rst.next()){
						System.out.println("Request is already put for download");
						Statement stat1 = con.createStatement();
						stat1.execute("select * from "+table);
						ResultSet resset=stat1.getResultSet();
						while(resset.next()){
							downloadRequest.add(resset.getString("contentId"));
						}
						resset.close();
						stat1.close();
					}
					else{
						System.out.println("Calling status'setDownloadRequest data insertion");
						prep = con.prepareStatement("insert into "+table+" values(?)");
						prep.setString(1,contentId);
						prep.execute();
						prep.close();
						Statement stat1 = con.createStatement();
						stat1.execute("select * from "+table);
						ResultSet resset=stat1.getResultSet();
						while(resset.next()){
							downloadRequest.add(resset.getString("contentId"));
						}
						resset.close();
						stat1.close();
					}
					rst.close();
					stat.close();
				} catch (Exception e) {
					System.out.println("Exception occurs at setDownloadRequests");
				}
				return downloadRequest;
				
			case 1: 
				try{
					System.out.println("Calling status'setDownloadRequest for data deletion");
					prep = con.prepareStatement("delete from "+table+" where contentId = ?");
					prep.setString(1, contentId);
					prep.execute();
					prep.close();
					stat = con.createStatement();
					stat.execute("select * from "+table);
					ResultSet resset=stat.getResultSet();
					while(resset.next()){
						downloadRequest.add(resset.getString("contentId"));
					}
					resset.close();
					stat.close();
				}catch(Exception e){
					System.out.println("Exception occurs at setDownloadRequests");
				}
				return downloadRequest;	
			
		}
		
		return downloadRequest;
	}
	public Map<String,String> setUploadRequests(String table, String contentId){
		Map<String,String> uploadRequestList = new HashMap<String,String>();
		try{
			
			prep = con.prepareStatement("delete from "+table+" where contentid = ? ");
			prep.setString(1,contentId);
			prep.execute();
			prep.close();
			stat = con.createStatement();
			stat.execute("select * from "+table);
			ResultSet resset = stat.getResultSet();
			while(resset.next()){
				uploadRequestList.put(resset.getString("contentid"),resset.getString("uploadId"));
			}
			resset.close();
			stat.close();
		}catch(Exception e){
			System.out.println("Exception occurs at setUploadRequests");
		}
		return uploadRequestList ;
	}

	
	public Map<String,String> getUploadRequsets(String table){
		Map<String,String> uploadRequestMap = new HashMap<String,String>();
		try{
			stat = con.createStatement();
			stat.execute("select * from "+table);
			ResultSet resset = stat.getResultSet();
			while(resset.next()){
				uploadRequestMap.put(resset.getString("contentid"),resset.getString("uploadid"));
			}
			resset.close();
			stat.close();
		}catch(Exception e){
			System.out.println("Exception occurs at getUploadRequests");
		}
		return uploadRequestMap ;
	}
	
	public List<String> getDownloadRequests(String table){
		List<String> downloadRequestList = new ArrayList<String>();
		try{
			stat = con.createStatement();
			stat.execute("select * from "+table);
			ResultSet resset = stat.getResultSet();
			while(resset.next()){
				downloadRequestList.add(resset.getString("contentid"));
			}
			resset.close();
			stat.close();
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("Exception occurs at getDownloadRequests");
		}
		return downloadRequestList ;
	}
	
	public void setuploadNotification(String table, String user, String data){
		try{
			prep = con.prepareStatement("insert into "+table+" values(?,?)");
			prep.setString(1, data);
			prep.setString(2,user);
			prep.execute();
			prep.close();
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("Exception occurs at setuploadNotification");
		}
	}
	
	public Map<String,ContentState> getPendingStatus(String query)
	{
		Map<String, ContentState> map = new HashMap<String, ContentState>();
		try
		{
			stat =  con.createStatement();
			stat.execute(query);
			ResultSet resset = stat.getResultSet();
			while(resset.next())
			{
				String contentId = resset.getString("contentId");
				int off = resset.getInt("off");
				int size = resset.getInt("totseg");
				BitSet bitSet = new BitSet(size);
				int prefInt = resset.getInt("prefint");
				String appId = resset.getString("appid");
				boolean meta = resset.getBoolean("sendmetadata");
				ContentState contentState = new ContentState(contentId,off,bitSet,prefInt,null,size,0,ContentState.Type.tcpDownload,appId,meta);
				map.put(contentId,contentState);
			}
			resset.close();
			stat.close();
		} catch (Exception e)
		{
			System.out.println("Exception occurs at getPendingStatus");
		}
		return map ;
	}
	public Map<String,List<ContentState>> getDownMap(List<String> request){
		Map<String, List<ContentState>> downMap = new HashMap<String, List<ContentState>>();
		
		for(int i = 0 ; i < request.size() ; i++){
			String content = request.get(i);
			List<ContentState> downlist = new ArrayList<ContentState>();
			try {
				stat = con.createStatement();
				stat.execute("select * from status where contentid ='"+content+"' and type = 1");
				ResultSet resset = stat.getResultSet();
				while(resset.next()){
					String contentId = resset.getString("contentId");
					int off = resset.getInt("off");
					int size = resset.getInt("totseg");
					BitSet bitSet = new BitSet(size);
					int prefInt = resset.getInt("prefint");
					String appId = resset.getString("appid");
					boolean meta = resset.getBoolean("sendmetadata");
					List<String> destinations = new ArrayList<String>();
					String dest = resset.getString("prefrt")+":"+resset.getInt("prefrtport");
					System.out.println("Destination in Status is: "+dest );
					destinations.add(dest);
					ContentState contentState = new ContentState(contentId,contentId,off,bitSet,prefInt,destinations,size,0,ContentState.Type.tcpUpload,appId,meta);
					downlist.add(contentState);
				}
				downMap.put(content,downlist);
				resset.close();
				stat.close();
			} catch (Exception e) {
				System.out.println("Exception occurs at getDownMap");
			}
			
		}
		return downMap ;
	}
	public String getContentType(String table,String data)
	{
		String str = "" ;
		ResultSet rs = null ;
		try {
			Statement stat = con.createStatement();
			stat.execute("select type from "+table+" where contentid ='"+data+"'");
			rs = stat.getResultSet();
			if(rs.next())
				str = rs.getString(1) ;
			rs.close();
			stat.close();
		} catch (Exception e) {	
			System.out.println("Exception occurs at getContentType");
			e.printStackTrace();
		}		
		return str ;
	}
	
	public boolean containsData(String table,String data)
	{
		boolean exists = false;
		ResultSet rs = null ;
		try {
			stat = con.createStatement();
			stat.execute("select * from "+table+" where contentid ='"+data+"'");
			rs = stat.getResultSet();
			if(rs.next())
				exists = true ;
			rs.close();
			stat.close();
		} catch (Exception e) {			
			System.out.println("Exception occurs at containsData");
		}	
		return exists ;
	}
	 public String updateUserLocation(String query, int insOrRet)
	 {
		 String userNode = "";
		 ResultSet resset = null ;
		 try
		 {
			 switch(insOrRet){
			 	case 1 :
			 		stat =  con.createStatement();
					stat.executeUpdate(query);
			 	    stat.close();
			 	    break ;
			 	
			 	case 2 :
			 		stat =  con.createStatement();
					stat.execute(query);
			 		resset = stat.getResultSet();
			 		if(resset.next())
			 		{
			 			String user = resset.getString(1);
			 			if(user != null)
			 				userNode = user ;
			 		}
			 		resset.close();
			 		stat.close();
			 		break;
		 	 }
		 }
		 catch(SQLException e)
		 {
			 System.err.println("SQL Exception occurs at updateUserLocation: " + e.getMessage());
		 }
		 System.out.println("value of userNode is: "+userNode);
		 return userNode ;
		 
	 }
	 
	 public String updateUserDaemonLocation(String IPAdd, String userId){
		 String userNode = "";
		 System.out.println("update userdaemonloc with value "+IPAdd+", "+userId);
		 try{
			 Statement stmt = con.createStatement();
			 stmt.executeQuery("select ipadd from userdaemonloc where userNode='"+userId+"'");
			 ResultSet rs = stmt.getResultSet();
			 if(rs.next()){
				 stmt.execute("update userdaemonloc set ipadd ='"+ IPAdd+"' where userNode ='"+userId+"'");
			 }
			 else
				 stmt.execute("insert into userdaemonloc values('"+userId+"','"+IPAdd+"')");
		 }catch(Exception e){
			 e.printStackTrace();
		 }
		 return userNode ;
		 
	 }
	 public String getUserLocation(String username){
		 String ipAdd = "" ;
		 ResultSet rs = null;
		 try{
			 Statement stmt = con.createStatement();
			 stmt.execute("select ipadd from userdaemonloc where usernode in (select usernode from userlocation where username ='"+username+"')" );
			 rs = stmt.getResultSet();
			 if(rs.next()){
				 ipAdd = rs.getString(1);
			 }
		 }catch(Exception e){
			 e.printStackTrace();
		 }
		 return ipAdd ;
		 
	 }
	 public String getUserDaemonLocation(String userNode){
		 String userdaemonIP = "";
		 ResultSet rs = null ;
		 try
		 {
			Statement stmt = con.createStatement();
			stmt.executeQuery("select ipadd from userdaemonloc where usernode = '"+userNode+"'");
			rs = stmt.getResultSet();
			if(rs.next())
				userdaemonIP = rs.getString(1) ;
		 }catch(Exception e){
			 e.printStackTrace();
			 
		 }
		 return userdaemonIP ;
	 }

}
	