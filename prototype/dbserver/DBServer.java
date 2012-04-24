package prototype.dbserver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.*;
import java.rmi.server.UnicastRemoteObject;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import newNetwork.Connection;
import AbstractAppConfig.AppConfig;
import NewStack.NewStack;
import StateManagement.ContentState;
import StateManagement.ServiceInstanceAppStateManager;
import StateManagement.StateManager;
import StateManagement.Status;
import prototype.datastore.DataStore;
import java.sql.*;

public class DBServer implements IDBServer 
{

	DataStore store;
	NewStack networkStack;
	StateManager stateManager;
	String dbServerId;
	BlockingQueue<String> fileDownloads;
	ServiceInstanceAppStateManager AppManager;
	static Map<String,BitSet> ContentBitMap;
	static Map<String,Integer> ContentSegCount;
	static List<String> UploadSyncList;
	static List<String> readingList ;
	static Map<String,List<String>> writingListMap ;
	private boolean dblock ;
	private String path ;
	java.sql.Connection con;
	String fileName ;
	public DBServer(String Id) throws Exception 
	{

		int port = Integer.parseInt(AppConfig.getProperty("DBServer.Port"));
		path = AppConfig.getProperty("DBServer.Directory.Path");         
		store = new DataStore(path);
		DataStore usbStore = null;
		if(AppConfig.getProperty("Routing.allowDTN").equals("1"))
		{
			usbStore = new DataStore(AppConfig.getProperty("DBServer.USBPath"));
		}
		readingList = new ArrayList<String>();
		writingListMap = new HashMap<String,List<String>>();
		fileName = "";
		dbServerId = Id;
		stateManager = new StateManager("status");
		AppManager = new ServiceInstanceAppStateManager();
		int maxDownloads = Integer.parseInt(AppConfig.getProperty("DBServer.MaximumDownloads"));
		BlockingQueue<String> fileDownloads = new ArrayBlockingQueue<String>(maxDownloads);   
		int clientPort = Integer.parseInt(AppConfig.getProperty("DBServer.NetworkStack.Port"));
		int tempPort = clientPort;
		List<Integer> cacheConnectionPorts = new ArrayList<Integer>(20);  
		for(int i = 0; i < 20;i++)
		{
			cacheConnectionPorts.add(tempPort);
			tempPort++;
		}
		networkStack = new NewStack(dbServerId,stateManager,store,usbStore,fileDownloads,port,cacheConnectionPorts,readingList,writingListMap);
		dblock = false ;
		Class.forName("com.mysql.jdbc.Driver");
		con = DriverManager.getConnection
			("jdbc:mysql://localhost:3306/ruralcdn","root","abc123");
		
		LogRegisterer registerer = new LogRegisterer(fileDownloads);
		registerer.start();
	}
	public boolean upload(String contentName, int size, String src) throws RemoteException{
		if(dblock)
			return false ;
		try
		{
			System.out.println("DBSync is not locked for contentName: "+contentName);
			fileName = contentName ;
			dblock = true ;
			//Added now
			/*IDataServer stub = null ;
			try{
				Registry registry = LocateRegistry.getRegistry("myDataServer");
				stub = (IDataServer) registry.lookup("dataserver");
				
			}catch( Exception e){
				
			}
			logFileToSend("myDataServer");
			int segments= networkStack.countSegments("upload.log");
			boolean flag = stub.upload(dbServerId+".log", segments, dbServerId);
			if(flag){
				List<String> route = new ArrayList<String>();
				route.add("myDataServer:5678:myDataServer");
				BitSet bitMap = new BitSet(segments);
				ContentState stateObject = new ContentState("upload.log",dbServerId+".log",0,bitMap, 
						Connection.Type.DSL.ordinal(),route,segments,0,ContentState.Type.tcpUpload,networkStack.getStackId(),true);
				stateManager.setTCPUploadState(stateObject,null);
				
				segments = stub.findLog("download.log", dbServerId+":6789");
				bitMap = new BitSet(segments);
				networkStack.addDestination("myDataServer:5678:myDataServer");
				List<String> destinations = new ArrayList<String>();
				destinations.add("myDataServer:5678:myDataServer");
				/*ContentState stateObject1 = new ContentState("download.log",0,bitMap,-1,destinations,segments/*size*///,0,ContentState.Type.tcpDownload,Integer.toString(1/*id*/),true);
			/*	stateManager.setStateObject(stateObject1); 

			}*/
			ContentState downloadStateObject = new ContentState(contentName,0,new BitSet(size),
					-1,null,size,0,ContentState.Type.tcpDownload,networkStack.getStackId(), true);
			stateManager.setStateObject(downloadStateObject);
			logFileToSend(src);
			return true ;
		}catch(Exception e)
		{
			e.printStackTrace();
			dblock = false ;
			return false;
		}

	}
	
	public boolean uploadThumb(String contentName, int size) throws RemoteException{
		try{
			ContentState downloadStateObject = new ContentState(contentName,0,new BitSet(size),
					-1,null,size,0,ContentState.Type.tcpDownload,networkStack.getStackId(), true);
			stateManager.setStateObject(downloadStateObject);
			return true ;
		}catch(Exception e){
			e.printStackTrace();
			return false ;
		}
	}
	
	public int findImg(String imgName, String userdaemon) throws RemoteException{
		try{
			List<String> destinations = new ArrayList<String>();
			destinations.add(userdaemon);
			int size = networkStack.countSegments(imgName);
			System.out.println("In find size is: "+size);
			System.out.println("Value of NewStack: "+networkStack.getStackId());
			ContentState uploadStateObject = new ContentState(imgName,imgName,0, new BitSet(size),
					Connection.Type.DSL.ordinal(),destinations,size,0,ContentState.Type.tcpUpload,networkStack.getStackId(),true);
			stateManager.setTCPDownloadState(uploadStateObject);
			return size;
		}catch(Exception e){
			e.printStackTrace();
			return -1;

		}
	}
	
	public void infoIP(String userId, String IPAdd) throws RemoteException{
		Status st = Status.getStatus();
		st.updateUserDaemonLocation(IPAdd,userId);
		
	}
	public int find(String dataname, String dest) throws RemoteException{
		try
		{
			List<String> destinations = new ArrayList<String>();
			destinations.add(dest);
			int size = networkStack.countSegments("upload.log");
			System.out.println("In find size is: "+size);
			System.out.println("Value of NewStack: "+networkStack.getStackId());
			ContentState uploadStateObject = new ContentState("upload.log",dataname,0, new BitSet(size),
					Connection.Type.DSL.ordinal(),destinations,size,0,ContentState.Type.tcpUpload,networkStack.getStackId(),true);
			stateManager.setTCPDownloadState(uploadStateObject);
			return size;
		}catch(Exception e)
		{
			System.out.println("Exception in DBServer's find(): "+e.toString());
			e.printStackTrace();
			return -1;
		}	

	}
	
	public void executeLog(String dest) throws RemoteException{
		try{
			System.out.println("In executeLog");
			
			//commented by amit
			
			executeLogStatement();
			updateLogFile(fileName,"cache_db.log",dest);
			File fileUp = new File(path+"upload.log");
			File fileSent = new File(path+fileName);
			if(fileUp.exists())
				fileUp.delete();
			if(fileSent.exists())
				fileSent.delete();
			dblock = false ;
		}catch(Exception e){
			e.printStackTrace();
		}

	}
	
	public void logFileToSend(String dest)throws Exception{
		//File toBeRead = new File(path+"/"+"cache_db.log");
		//File toBeSent = new File(path+"/"+"upload.log");
		File toBeRead = new File(path+"cache_db.log");
		File toBeSent = new File(path+"upload.log");
		Statement stmt = con.createStatement();
		ResultSet rs = null;
		stmt.execute("select * from synctable where entity ='"+dest+"'");
		rs = stmt.getResultSet();
		int cp = 0 ;
		if(rs.next())
			cp = rs.getInt(2);
		rs.close();
		BufferedReader reader = new BufferedReader(new FileReader(toBeRead)); 
		BufferedWriter writer = new BufferedWriter(new FileWriter(toBeSent));
		String str;
		int count = 0 ;
		boolean blank = true;
		while((str = reader.readLine())!=null){
			++count;
			if(count<=cp){
				continue ;
			}	
			else{
				
				writer.write(str);
				writer.newLine();
				blank = false;
			}
		}
		if(blank)
			writer.newLine();
		reader.close();
		writer.close();
		
	}
	
	//public void executeLogStatement(File logFile) throws Exception{
	public void executeLogStatement() throws Exception{
		File logFile = new File(path+fileName);
		BufferedReader reader = new BufferedReader(new FileReader(logFile)); 
		String str ;
		Statement stmt = con.createStatement();
		while((str=reader.readLine())!=null && (str.startsWith("insert")||str.startsWith("update")||str.startsWith("delete"))){
			stmt.execute(str);
		}
		stmt.close();
		reader.close();
	}
	
	public void updateLogFile(String downFile, String logFile, String Client){
		try{
			File cache_log = new File(path+logFile);
			File temp = new File(path+downFile);
			BufferedReader reader = new BufferedReader(new FileReader(temp)); 
			BufferedWriter writer = new BufferedWriter(new FileWriter(cache_log,true));
			String str;
			while((str = reader.readLine())!=null && str.length()!=0 &&(str.startsWith("insert")||str.startsWith("update")||str.startsWith("delete"))){
				writer.write(str);
				writer.newLine();
				
			}
			reader.close();
			writer.close();
			BufferedReader reader1 = new BufferedReader(new FileReader(cache_log));
			int count = 0 ;
			while(reader1.readLine()!= null)
				++count ;
			Statement stmt = con.createStatement();
			stmt.execute("update synctable set updated_till ="+count+" where entity ='"+Client+"'");
			//System.out.println("Logs are updated with checkpoint: "+count+" for the client: "+Client);
			stmt.close();
			reader1.close();
			
		}catch(Exception e)
		{
			e.printStackTrace();
		}
		
	}

	public List<Integer> getUploadAcks(String contentName, int size) throws RemoteException{
		List<Integer> pendingPackets = new ArrayList<Integer>();
		Map<String, ContentState> mpContent = StateManager.getDownMap();
		ContentState contentState = mpContent.get(contentName);
		if(contentState != null){
			BitSet bitSet = contentState.bitMap;
			for(int i = 0 ; i < size ; i++){
				if(bitSet.get(i)==false)
					pendingPackets.add(i);
			}
		}
		System.out.println("Sending acks for pending packets for the content: "+contentName+" with pending packets: "+pendingPackets.size());
		return pendingPackets ;
	}
	
	public static void main(String args[])
	{ 

		try 
		{
			String config = new String("config/DBServer.cfg");
			File configFile = new File(config);
			FileInputStream fis;
			fis = new FileInputStream(configFile);
			new AppConfig();
			AppConfig.load(fis);			
			fis.close();
			System.getProperties().setProperty("java.rmi.server.hostname","myrsyncserver");
			String Id = AppConfig.getProperty("DBServer.Id");
			DBServer obj = new DBServer(Id);
			IDBServer stub = (IDBServer) UnicastRemoteObject.exportObject(obj, 0);
			Registry registry = LocateRegistry.getRegistry();
			boolean found = false;
			while(!found)
			{
				try
				{
					registry.bind(AppConfig.getProperty("DBServer.Service"), stub);
					found = true;
				}
				catch(AlreadyBoundException ex)
				{
					registry.unbind(AppConfig.getProperty("DBServer.Service"));
					registry.bind(AppConfig.getProperty("DBServer.Service"), stub);
					found = true;
				}
				catch(ConnectException ex)
				{
					String rmiPath = AppConfig.getProperty("DBServer.Directory.rmiregistry");
					Runtime.getRuntime().exec(rmiPath);
				}
			}	
			
			System.err.println("Server ready");
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

	}
}



