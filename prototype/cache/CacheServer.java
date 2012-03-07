package prototype.cache;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.*;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import newNetwork.Connection;
import AbstractAppConfig.AppConfig;
//import DBSync.DBSync;
import NewStack.NewStack;
import StateManagement.ContentState;
import StateManagement.ServiceInstanceAppStateManager;
import StateManagement.StateManager;
import prototype.datastore.DataStore;
import prototype.rendezvous.IRendezvous;
import prototype.serviceinstance.IUploader;
import prototype.dataserver.IDataServer ;

public class CacheServer implements ICacheServer 
{

	DataStore store;
	NewStack networkStack;
	StateManager stateManager;
	String cacheId;
	BlockingQueue<String> fileDownloads;
	ServiceInstanceAppStateManager AppManager;
	static Map<String,BitSet> ContentBitMap;
	static Map<String,Integer> ContentSegCount;
	static List<String> UploadSyncList;
	List<String> readingList ; //For DTN receiver 
	//DBSync dbSync ;
	public CacheServer(String Id) throws Exception 
	{

		int port = Integer.parseInt(AppConfig.getProperty("CacheServer.Port"));
		String path = AppConfig.getProperty("CacheServer.Directory.Path");         
		store = new DataStore(path);
		DataStore usbStore = null;
		if(AppConfig.getProperty("Routing.allowDTN").equals("1"))
		{
			usbStore = new DataStore(AppConfig.getProperty("CacheServer.USBPath"));
		}
		cacheId = Id;
				
		stateManager = new StateManager("status");
		AppManager = new ServiceInstanceAppStateManager();
		UploadSyncList = new ArrayList<String>();
		readingList = new ArrayList<String>();
		//dbSync = new DBSync(6789);
		int maxDownloads = Integer.parseInt(AppConfig.getProperty("CacheServer.MaximumDownloads"));
		BlockingQueue<String> fileDownloads = new ArrayBlockingQueue<String>(maxDownloads);   
		//FileRegisterer registerer = new FileRegisterer(fileDownloads,AppManager);
		//FileRegisterer registerer = new FileRegisterer(fileDownloads,AppManager, cacheId);
		FileRegisterer registerer = new FileRegisterer(fileDownloads, cacheId);
		registerer.start();
		
		int clientPort = Integer.parseInt(AppConfig.getProperty("CacheServer.NetworkStack.Port"));
		int tempPort = clientPort;
		List<Integer> cacheConnectionPorts = new ArrayList<Integer>(20);  
		for(int i = 0; i < 20;i++)
		{
			cacheConnectionPorts.add(tempPort);
			tempPort++;
		}
		//networkStack = new NewStack(cacheId,stateManager,store,usbStore,fileDownloads,port,cacheConnectionPorts);
		networkStack = new NewStack(cacheId,stateManager,store,usbStore,fileDownloads,port,cacheConnectionPorts,readingList);
		
	}

	public  String upload(String userContentName,int size,String dest, String userId, String fileType) throws RemoteException
	{
		System.out.println("Inside CacheServer.java: upload");
		System.out.println("destination: " + dest);
		String contentName = null;
		try
		{
			System.out.println("New Data To be Uploaded with size: "+size);
			
			Registry registry = LocateRegistry.getRegistry(AppConfig.getProperty("CacheServer.Rendezvous.IP"));    
			String rendezvousService = AppConfig.getProperty("CacheServer.Rendezvous.Service");
			
			IRendezvous stub = null;
			
			System.out.println("Finding Rendezvous Service.");			
			
			try
			{
				stub = (IRendezvous) registry.lookup(rendezvousService);
				
			}
			catch(Exception ex)
			{
				return "RendezvousNotFound";
				
			}
			
			System.out.println("Rendezvous Service Found.");				
			
			List<String> l = stub.find(dest+"$serviceInstance");
			String serviceInstanceInfo = l.get(0);
			System.out.println("response from rendezvous:  "+l);	

			File f = new File("config/output.cfg");
			FileOutputStream fop = new FileOutputStream(f);;				
			if(f.exists())
			{
		          String str=l.toString();
		          fop.write(str.getBytes());
		          fop.flush();
		          fop.close();
			}
			
			
			String[] connectionInfo = serviceInstanceInfo.split(":");
			String IP = connectionInfo[0];

			System.out.println("IP = "+IP);
			
			Registry serverRegistry = LocateRegistry.getRegistry(IP);
			IUploader serverStub = null;
			
			System.out.println("Finding ServiceInstance.");			
			
			try
			{
				serverStub = (IUploader) serverRegistry.lookup(AppConfig.getProperty("CacheServer.ServiceInstance.UploaderService") );    //should be config driven
			
			}
			catch(Exception ex)
			{
				
				return "ServiceInstanceNotFound";
			}
			
			System.out.println("ServiceInstance Found.");	
			
			
			System.out.println("Calling serverStub.upload");
			contentName = serverStub.generateContentId();
			
			//int count = contentName.indexOf('$'); //Modified in 19-03-2011
			//userContentName = userContentName+"$"+contentName.substring(count+1);
			
						
			Registry dataServerRegistry = LocateRegistry.getRegistry("myDataServer");
			IDataServer dataServerStub = null;
			
			System.out.println("Finding DataService.");	
						
			try
			{
				dataServerStub = (IDataServer) dataServerRegistry.lookup(AppConfig.getProperty("CacheServer.DataServer.UploaderService") );    //should be config driven			
			}
			catch(Exception ex)
			{
				return "DataServerNotFound";
			}

			System.out.println("DataService Found.");		
			
			System.out.println("Calling dataServerStub.upload(): CacheServer.java");
            //dataServerStub.upload(contentName, size, userId);
            dataServerStub.upload(contentName, size, userId,fileType);
            System.out.println("After Calling dataServerStub.upload(): CacheServer.java");
            List<String> destinations = new ArrayList<String>();
			destinations.add("myDataServer:5678");
			//ContentState downloadStateObject = new ContentState(userContentName,0,new BitSet(size),
				//	-1,null,size,0,ContentState.Type.tcpDownload,networkStack.getStackId(), true);
			ContentState downloadStateObject = new ContentState(contentName,0,new BitSet(size),
					-1,null,size,0,ContentState.Type.tcpDownload,networkStack.getStackId(), true);
			
			//ContentState uploadStateObject = new ContentState(userContentName,contentName,0,new BitSet(size),
					//Connection.Type.DSL.ordinal(),destinations,size,0,ContentState.Type.tcpUpload,networkStack.getStackId(),true);
			ContentState uploadStateObject = new ContentState(contentName,contentName,0,new BitSet(size),
					Connection.Type.DSL.ordinal(),destinations,size,0,ContentState.Type.tcpUpload,networkStack.getStackId(),true);
			String fullName = contentName +"."+fileType;
			UploadSyncList.add(fullName);///Added 19-03-2011
			stateManager.setStateObject(downloadStateObject);
			stateManager.setTCPUploadState(uploadStateObject, fileType);
		}catch(Exception e)
		{
			e.printStackTrace();
			return contentName;
		}
		return contentName;	
	}
	
	public  String dtnUpload(String userContentName,int size,String dest, String userId, String fileType) throws RemoteException
	{
		System.out.println("Inside CacheServer.java: DTNUpload");
		System.out.println("destination: " + dest);
		String contentName = null;
		try
		{
			System.out.println("New Data To be Uploaded with size: "+size);
			
			Registry registry = LocateRegistry.getRegistry(AppConfig.getProperty("CacheServer.Rendezvous.IP"));    
			String rendezvousService = AppConfig.getProperty("CacheServer.Rendezvous.Service");
			
			IRendezvous stub = null;
			
			System.out.println("Finding Rendezvous Service.");			
			
			try
			{
				stub = (IRendezvous) registry.lookup(rendezvousService);
				
			}
			catch(Exception ex)
			{
				return "RendezvousNotFound";
				
			}
			
			System.out.println("Rendezvous Service Found.");				
			
			List<String> l = stub.find(dest+"$serviceInstance");
			String serviceInstanceInfo = l.get(0);
			System.out.println("response from rendezvous:  "+l);	

			File f = new File("config/output.cfg");
			FileOutputStream fop = new FileOutputStream(f);;				
			if(f.exists())
			{
		          String str=l.toString();
		          fop.write(str.getBytes());
		          fop.flush();
		          fop.close();
			}
			
			
			String[] connectionInfo = serviceInstanceInfo.split(":");
			String IP = connectionInfo[0];

			System.out.println("IP = "+IP);
			
			Registry serverRegistry = LocateRegistry.getRegistry(IP);
			IUploader serverStub = null;
			
			System.out.println("Finding ServiceInstance.");			
			
			try
			{
				serverStub = (IUploader) serverRegistry.lookup(AppConfig.getProperty("CacheServer.ServiceInstance.UploaderService") );    //should be config driven
			
			}
			catch(Exception ex)
			{
				
				return "ServiceInstanceNotFound";
			}
			
			System.out.println("ServiceInstance Found.");	
			
			
			System.out.println("Calling serverStub.upload");
			contentName = serverStub.generateContentId();
			
			int count = contentName.indexOf('$'); //Modified in 19-03-2011
			userContentName = userContentName+"$"+contentName.substring(count+1);
			
						
			Registry dataServerRegistry = LocateRegistry.getRegistry("myDataServer");
			IDataServer dataServerStub = null;
			
			System.out.println("Finding DataService.");	
						
			try
			{
				dataServerStub = (IDataServer) dataServerRegistry.lookup(AppConfig.getProperty("CacheServer.DataServer.UploaderService") );    //should be config driven			
			}
			catch(Exception ex)
			{
				return "DataServerNotFound";
			}

			System.out.println("DataService Found.");		
			
			System.out.println("Calling dataServerStub.upload(): CacheServer.java");
            //dataServerStub.upload(contentName, size, userId);
            dataServerStub.upload(contentName, size, userId,fileType);
            System.out.println("After Calling dataServerStub.upload(): CacheServer.java");
            List<String> destinations = new ArrayList<String>();
			destinations.add("myDataServer:5678");
			ContentState downloadStateObject = new ContentState(contentName,0,new BitSet(size),
					-1,null,size,0,ContentState.Type.tcpDownload,networkStack.getStackId(), true);
			
			ContentState uploadStateObject = new ContentState(contentName,contentName,0,new BitSet(size),
					Connection.Type.DSL.ordinal(),destinations,size,0,ContentState.Type.tcpUpload,networkStack.getStackId(),true);
			String fullName = contentName +"."+fileType;
			UploadSyncList.add(fullName);///Added 19-03-2011
			readingList.add(contentName);
			stateManager.setStateObject(downloadStateObject);
			stateManager.setTCPUploadState(uploadStateObject, fileType);
		}catch(Exception e)
		{
			e.printStackTrace();
			return contentName;
		}
		return contentName;	
	}
	
	public int TCPRead(int AppId,String dataname,Connection.Type type,String conId) throws RemoteException
	{
		if(store.contains(dataname) && store.contains(dataname+".marker"))
		{
			return networkStack.countSegments(dataname);
		}
		else
			return -1;
	}

	public boolean DTNRead(int AppId,String dataname,String dataRequester,String conId) throws RemoteException
	{
	
		if(store.contains(dataname))
			return true;
		else
			return false;		
	}
	
	public long find(int AppId,String dataname,Connection.Type type, String dest) throws RemoteException
	{
		
		System.out.println("Find request received by user on "+dataname);
		if(store.contains(dataname))
		{
			if(type == Connection.Type.USB)
			{
				List<String> destinations = new ArrayList<String>();
				destinations.add(dest);
				int size = networkStack.countSegments(dataname);
				ContentState uploadStateObject = new ContentState(dataname,dataname,0, new BitSet(size),
						Connection.Type.USB.ordinal(),destinations,size,0,ContentState.Type.dtn,networkStack.getStackId(),true);
				//stateManager.setTCPDownloadState(uploadStateObject);
				stateManager.setDTNDownState(uploadStateObject);
				//return size;
				return 1;
			}
			else
			{
				System.out.println("Data is already available in cacheServer");
				List<String> destinations = new ArrayList<String>();
				destinations.add(dest);
				int size = networkStack.countSegments(dataname);
				ContentState uploadStateObject = new ContentState(dataname,dataname,0, new BitSet(size),
						Connection.Type.DSL.ordinal(),destinations,size,0,ContentState.Type.tcpUpload,networkStack.getStackId(),true);
				stateManager.setTCPDownloadState(uploadStateObject);
				return size;
			}
		}
		else
		{
			try
			{
				System.out.println("Requesting rendezvous");
				Registry registry = LocateRegistry.getRegistry(AppConfig.getProperty("CacheServer.Rendezvous.IP"));    
				String rendezvousService = AppConfig.getProperty("CacheServer.Rendezvous.Service");
				IRendezvous stub = (IRendezvous) registry.lookup(rendezvousService);   
				List<String> l = stub.find(dataname);
				System.out.println("response from rendezvous:    "+l);					
								
				File f = new File("config/output.cfg");
				FileOutputStream fop = new FileOutputStream(f);;				
				if(f.exists())
				{
			          String str=l.toString();
			          fop.write(str.getBytes());
			          fop.flush();
			          fop.close();
				}
								
				int size = 1;
				if(l != null)
				{
					String cache = l.get(0);
					String[] cacheInformation = cache.split(":");
					String IP = cacheInformation[0];
					Registry cacheRegistry = LocateRegistry.getRegistry(IP);
					IDataServer cacheStub = (IDataServer) cacheRegistry.lookup(AppConfig.getProperty("CacheServer.DataServer.UploaderService") );    //should be config driven
					
					if(type != Connection.Type.USB)
					{
						List<String> destinations = new ArrayList<String>();
						destinations.add(dest);
						System.out.println("Value of NewStack: "+networkStack.getStackId());
						ContentState stateObject = stateManager.getStateObject(dataname, ContentState.Type.tcpDownload);
						if(stateObject == null){
							System.out.println("StateObject is null");
							size = cacheStub.TCPRead(1,dataname,type,networkStack.getStackId()+":8700");
							ContentState downloadStateObject = new ContentState(dataname,0,new BitSet(size),
									-1,null,size,0,ContentState.Type.tcpDownload,networkStack.getStackId(),true);
							stateManager.setStateObject(downloadStateObject);
						}
						else{
							System.out.println("StateObject is not null");
							size = stateObject.getTotalSegments();
						}
						ContentState uploadStateObject = new ContentState(dataname,dataname,0, new BitSet(size),
								Connection.Type.DSL.ordinal(),destinations,size,0,ContentState.Type.tcpUpload,networkStack.getStackId(),true);
						stateManager.setTCPDownloadState(uploadStateObject);
						return size;
					}
					else
					{
						//if(cacheStub.DTNRead(1,dataname,userId,networkStack.getStackId()))
						if(cacheStub.DTNRead(1,dataname,"user",networkStack.getStackId()))
							return 1;
						else
							return -1;
					}

				}
				else
					return -1;

			}catch(Exception e)
			{
				System.out.println("Exception in contacting rendezvous"+e.toString());
				e.printStackTrace();
				return -1;
			}	
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
	
	public void notify(String data) throws RemoteException
	{
		System.out.println("Notification for "+ data +" downloading received");
	}

	public static void main(String args[])
	{ 

		try 
		{
			String config = new String("config/CacheServer.cfg");
			File configFile = new File(config);
			FileInputStream fis;
			fis = new FileInputStream(configFile);
			new AppConfig();
			AppConfig.load(fis);			
			fis.close();
			String Id = AppConfig.getProperty("CacheServer.Id");
			CacheServer obj = new CacheServer(Id);
			ICacheServer stub = (ICacheServer) UnicastRemoteObject.exportObject(obj, 0);

			Registry registry = LocateRegistry.getRegistry();
			boolean found = false;
			while(!found)
			{
				try
				{
					registry.bind(AppConfig.getProperty("CacheServer.Service"), stub);
					found = true;
				}
				catch(AlreadyBoundException ex)
				{
					registry.unbind(AppConfig.getProperty("CacheServer.Service"));
					registry.bind(AppConfig.getProperty("CacheServer.Service"), stub);
					found = true;
				}
				catch(ConnectException ex)
				{
					Runtime.getRuntime().exec("C:\\Program Files\\Java\\jdk1.6.0_16\\bin\\rmiregistry.exe");
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



