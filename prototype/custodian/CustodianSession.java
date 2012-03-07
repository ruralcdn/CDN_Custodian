package prototype.custodian;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import newNetwork.Connection;
import AbstractAppConfig.AppConfig;
import NewStack.NewStack;
import PubSubModule.Notification;
import StateManagement.ContentState;
import StateManagement.CustodianAppStateManager;
import StateManagement.StateManager;
import StateManagement.Status;
import prototype.rendezvous.IRendezvous;
import prototype.serviceinstance.IUploader;
import prototype.userregistrar.IUserRegistrarSession;
import prototype.utils.Utils;
import prototype.cache.ICacheServer;
import prototype.dataserver.IDataServer;
import prototype.datastore.DataStore;


@SuppressWarnings("unused")
public class CustodianSession implements ICustodianSession{

	String userId;   												
	IUserRegistrarSession sessionStub;							
	DataStore store;											
	NewStack networkStack;
	StateManager stateManager;
	CustodianAppStateManager appStateManager;
	List<String> UploadSyncList;
	List<String> readingList ;
	//public CustodianSession(String u,IUserRegistrarSession stub,StateManager sManager,CustodianAppStateManager caManager
		//	,DataStore st,NewStack stack)
	public CustodianSession(String u,IUserRegistrarSession stub,StateManager sManager,CustodianAppStateManager caManager
			,DataStore st,NewStack stack, List<String> readList, List<String> uploadSync)
	{
		userId = u;
		sessionStub = stub;
		store = st;
		stateManager = sManager;
		appStateManager = caManager;
		networkStack = stack;
		UploadSyncList = uploadSync;
		readingList = readList ;
		System.out.println("Session established userId: "+userId);
		String config = new String("config/Custodian.cfg");
		File configFile = new File(config);
		FileInputStream fis;
		try {
			fis = new FileInputStream(configFile);
			new AppConfig();
			AppConfig.load(fis);			
			fis.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	public boolean subscribe(String subject) throws RemoteException
	{
		return true;
	}

	public boolean unsubscribe(String subject) throws RemoteException
	{
		return true;
	}

	public boolean delete(String ContentId) throws RemoteException{
		boolean flag = false ;
		if(store.contains(ContentId)){
			store.delete(ContentId);
			Status st = Status.getStatus();
			st.delData("delete from localData where contentid = '"+ContentId+"'");
		}	
		try
		{
			System.out.println("Requesting rendezvous in delete method");
			Registry registry = LocateRegistry.getRegistry(AppConfig.getProperty("Custodian.Rendezvous.IP"));    
			String rendezvousService = AppConfig.getProperty("Custodian.Rendezvous.Service");
			IRendezvous stub = (IRendezvous) registry.lookup(rendezvousService);   
			List<String> l = stub.find(ContentId);
			System.out.println("response from rendezvous: "+l);					
			if(l != null)
			{
				String cache = l.get(0);
				for(int i = 0 ; i < l.size(); i++){
					String test = l.get(i);
					if(test.contains("DataServer")){
						cache = test ;
						break;
					}	
				}
				String[] cacheInformation = cache.split(":");
				String IP = cacheInformation[0];
				Registry cacheRegistry = LocateRegistry.getRegistry(IP);
				IDataServer cacheStub = (IDataServer) cacheRegistry.lookup(AppConfig.getProperty("Custodian.DataServer.Service") );
				cacheStub.delete(ContentId);
			}	
		}catch(Exception e){
			e.printStackTrace();
		}
		return flag ;
	}
	public List<Notification> poll_notification() throws RemoteException
	{
		List<Notification> notificationList = new ArrayList<Notification>();
		try
		{
			List<String> uploadList = appStateManager.getUploadNotification(userId);
			Iterator<String> it = uploadList.iterator();
			while(it.hasNext())
			{
				Notification notification = new Notification(Notification.Type.UploadAck,it.next());
				it.remove();
				notificationList.add(notification);
			}
			appStateManager.setUploadNotification(userId, new String(""));
			return notificationList;
		}
		catch(Exception e)
		{
			e.printStackTrace();
			return notificationList;
		}
	}

	public boolean close_connection() throws RemoteException
	{

		System.out.println("Closing Connection");
		sessionStub.close_connection();
		networkStack.close();
		return UnicastRemoteObject.unexportObject(this, true);

	}
	
	public List<String> processDynamicContent(String uploadContentId,int uploadSize,String replyContentId,Connection.Type uploadType,Connection.Type downloadType,String dest) throws RemoteException
	{
		List<String> reply = null;
		try
		{
			System.out.println("New Process Request for Dynamic Content");
			Registry registry = LocateRegistry.getRegistry(AppConfig.getProperty("Custodian.Rendezvous.IP"));    
			String rendezvousService = AppConfig.getProperty("Custodian.Rendezvous.Service");
			IRendezvous stub = (IRendezvous) registry.lookup(rendezvousService);  
			List<String> l = stub.find(dest+"$serviceInstance");
			String serviceInstanceInfo = l.get(0);
			System.out.println("response from rendezvous: "+l);	

			String[] connectionInfo = serviceInstanceInfo.split(":");
			String cacheAddress = connectionInfo[0];

			Registry serverRegistry = LocateRegistry.getRegistry(cacheAddress);
			IUploader serverStub = (IUploader) serverRegistry.lookup(AppConfig.getProperty("Custodian.ServiceInstance.UploaderService") );    //should be config driven
			int size = serverStub.processDynamicContent(uploadContentId,uploadSize, replyContentId,downloadType,userId,networkStack.getStackId()+":"+networkStack.getServerPort());
			if(uploadType != Connection.Type.USB)
			{
				char[] bits = new char[uploadSize];
				for(int i = 0;i < uploadSize;i++)
					bits[i] = '0';
				String bitMap = new String(bits);
				List<String> destinations = new ArrayList<String>();
				destinations.add(serviceInstanceInfo);

				ContentState downloadStateObject = new ContentState(uploadContentId,0,new BitSet(size),
						uploadType.ordinal(),destinations,-1,0,ContentState.Type.tcpDownload,networkStack.getStackId(),true);
				ContentState uploadStateObject = new ContentState(uploadContentId,uploadContentId,0,new BitSet(size),
						uploadType.ordinal(),destinations,size,0,ContentState.Type.tcpUpload,networkStack.getStackId(),true);
				stateManager.setStateObject(downloadStateObject);
				stateManager.setTCPUploadState(uploadStateObject,null);
				String bitMap1;
				char[] bits1 = new char[size];
				
				for(int i = 0;i < size;i++)
				{
					bits1[i] = '0';
				}
				bitMap1 = new String(bits1);     
				ContentState replyStateObject = new ContentState(replyContentId,0,new BitSet(size),-1,null,size,0,ContentState.Type.tcpDownload,networkStack.getStackId(),true);
				stateManager.setStateObject(replyStateObject);
				
			}

			reply = new ArrayList<String>();
			reply.add(Integer.toString(size));
			reply.add(serviceInstanceInfo);
			return reply;

		}catch(Exception e)
		{
			e.printStackTrace();
			return reply;
		}
	}
	
	

	public  String upload(String userContentName,int size,String dest, String userId, String fileType) throws RemoteException
	{
		System.out.println("Inside CacheServer.java: upload");
		System.out.println("destination: " + dest);
		String contentName = null;
		try
		{
			Registry registry = LocateRegistry.getRegistry(AppConfig.getProperty("Custodian.Rendezvous.IP"));    
			String rendezvousService = AppConfig.getProperty("Custodian.Rendezvous.Service");
			
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
				serverStub = (IUploader) serverRegistry.lookup(AppConfig.getProperty("Custodian.ServiceInstance.UploaderService") );    //should be config driven
			
			}
			catch(Exception ex)
			{
				return "ServiceInstanceNotFound";
			}
			
			System.out.println("ServiceInstance Found.");	
			System.out.println("Calling serverStub.upload");
			contentName = serverStub.generateContentId();
			//Registry dataServerRegistry = LocateRegistry.getRegistry("myDataServer");
			Registry dataServerRegistry = LocateRegistry.getRegistry(AppConfig.getProperty("Custodian.DataConnection.Server"));
			IDataServer dataServerStub = null;
			
			System.out.println("Finding DataService.");	
						
			try
			{
				dataServerStub = (IDataServer) dataServerRegistry.lookup(AppConfig.getProperty("Custodian.DataServer.Service") );    //should be config driven			
			}
			catch(Exception ex)
			{
				return "DataServerNotFound";
			}

			System.out.println("DataService Found.");		
			
			// upload of DataServer called
			dataServerStub.upload(contentName, size, userId,fileType);
            
			System.out.println("After Calling dataServerStub.upload(): CacheServer.java");
            List<String> destinations = new ArrayList<String>();
			String hop = AppConfig.getProperty("Custodian.DataConnection.Server")+":"+AppConfig.getProperty("Custodian.DataConnection.port");
            destinations.add(hop);
            
			ContentState downloadStateObject = new ContentState(contentName,0,new BitSet(size),
					-1,null,size,0,ContentState.Type.tcpDownload,networkStack.getStackId(), true);
			ContentState uploadStateObject = new ContentState(contentName,contentName,0,new BitSet(size),
					Connection.Type.DSL.ordinal(),destinations,size,0,ContentState.Type.tcpUpload,networkStack.getStackId(),true);
			String fullName = contentName +"."+fileType;
			//UploadSyncList.add(fullName);
			stateManager.setStateObject(downloadStateObject);
			stateManager.setTCPUploadState(uploadStateObject, fileType);
		}catch(Exception e)
		{
			e.printStackTrace();
			return contentName;
		}
		return contentName;	
	}
	
	public  String DTNUpload(String userContentName,int size,int smSize,String dest, String userId, String fileType) throws RemoteException
	{
		System.out.println("Inside CustodianSession.java: DTNUpload");
		String contentName = null;
		try
		{
			//size = size*10;
			int dtnSize = Integer.parseInt(AppConfig.getProperty("NetworkStack.dtnSegmentSize"));
			int tcpSize = Integer.parseInt(AppConfig.getProperty("NetworkStack.SegmentSize"));
			int scale = dtnSize/tcpSize ;
			//System.out.println("Segments to be received: "+size+" segments to be upload: "+scale*size);
			System.out.println("Segments to be received: "+size+" segments to be upload: "+smSize);
			Registry registry = LocateRegistry.getRegistry(AppConfig.getProperty("Custodian.Rendezvous.IP"));    
			String rendezvousService = AppConfig.getProperty("Custodian.Rendezvous.Service");
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
			String serviceInstanceInfo = "";
			try{
				serviceInstanceInfo = l.get(0);
			}catch(Exception ex){
				System.out.println("Exception Obtaining Service Instance Information in Custodian Session.");	
			}
			
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
				serverStub = (IUploader) serverRegistry.lookup(AppConfig.getProperty("Custodian.ServiceInstance.UploaderService") );    //should be config driven
			
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
				return "ServiceInstanceNotFound";
			}
			
			System.out.println("ServiceInstance Found.");	
			contentName = serverStub.generateContentId();
			int count = contentName.indexOf('$'); 
			userContentName = userContentName+"$"+contentName.substring(count+1);
			Registry dataServerRegistry = LocateRegistry.getRegistry("myDataServer");
			IDataServer dataServerStub = null;
			System.out.println("Finding DataService.");	
			try
			{
				dataServerStub = (IDataServer) dataServerRegistry.lookup(AppConfig.getProperty("Custodian.DataServer.Service") );    //should be config driven			
			}
			catch(Exception ex)
			{
				return "DataServerNotFound";
			}

			System.out.println("DataService Found.");		
			//dataServerStub.upload(contentName, size*scale, userId,fileType);
			dataServerStub.upload(contentName, smSize, userId,fileType);
            System.out.println("After Calling dataServerStub.upload(): CacheServer.java");
            List<String> destinations = new ArrayList<String>();
            String hop = AppConfig.getProperty("Custodian.DataConnection.Server")+":"+AppConfig.getProperty("Custodian.DataConnection.port");
           	destinations.add(hop);
			ContentState downloadStateObject = new ContentState(contentName,0,new BitSet(size),
					-1,null,size,0,ContentState.Type.tcpDownload,networkStack.getStackId(), true);
			/*ContentState uploadStateObject = new ContentState(contentName,contentName,0,new BitSet(size*scale),
					Connection.Type.DSL.ordinal(),destinations,size*scale,0,ContentState.Type.tcpUpload,networkStack.getStackId(),true);*/
			ContentState uploadStateObject = new ContentState(contentName,contentName,0,new BitSet(smSize),
					Connection.Type.DSL.ordinal(),destinations,smSize,0,ContentState.Type.tcpUpload,networkStack.getStackId(),true);
			String fullName = contentName +"."+fileType;
			//UploadSyncList.add(fullName);
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
	private void notifyNeighborCaches(String contentName)
	{
		try 
		{
			String neighborList = AppConfig.getProperty("Custodian.RelativeCaches");
			List<String> caches = Utils.parse(neighborList);
			Iterator<String> it = caches.iterator();
			while(it.hasNext())
			{
				String cacheInfo = it.next();
				it.remove();
				String[] connectionInfo = cacheInfo.split(":");
				String IP = connectionInfo[0];

				Registry serverRegistry;
				serverRegistry = LocateRegistry.getRegistry(IP);
				ICacheServer serverStub = (ICacheServer) serverRegistry.lookup(AppConfig.getProperty("Custodian.CacheService") );    //should be config driven
				serverStub.notify(contentName);

			}

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

	}

	public long find(int AppId,String dataname,Connection.Type type, String dest, int popContent) throws RemoteException
	{
		
		System.out.println("Find request received by user on "+dataname);
		if(store.contains(dataname) || popContent == 0)
		{
			if(popContent == 0){
				
				return 1 ;
			}
				
			if(type == Connection.Type.USB)
			{
				List<String> destinations = new ArrayList<String>();
				destinations.add(dest);
				int size = networkStack.countdtnSegments(dataname);
				ContentState uploadStateObject = new ContentState(dataname,dataname,0, new BitSet(size),
						Connection.Type.USB.ordinal(),destinations,size,0,ContentState.Type.dtn,networkStack.getStackId(),true);
				stateManager.setDTNDownState(uploadStateObject);
				//return 1;
				return size ;
			}
			else
			{
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
				Registry registry = LocateRegistry.getRegistry(AppConfig.getProperty("Custodian.Rendezvous.IP"));    
				String rendezvousService = AppConfig.getProperty("Custodian.Rendezvous.Service");
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
					String cache = null ;
					for(int i = 0; i < l.size() ; i++){
						String temp = l.get(i);
						if(temp.contains("myDataServer")){
							cache = temp ;
							break ;
						}	 
					} 
					System.out.println("cache is -> "+cache);
					String[] cacheInformation = cache.split(":");
					String IP = cacheInformation[0];
					Registry cacheRegistry = LocateRegistry.getRegistry(IP);
					IDataServer cacheStub = (IDataServer) cacheRegistry.lookup(AppConfig.getProperty("Custodian.DataServer.Service") );    //should be config driven
					
					if(type != Connection.Type.USB)
					{
						List<String> destinations = new ArrayList<String>();
						destinations.add(dest);
						System.out.println("Value of NewStack: "+networkStack.getStackId());
						ContentState stateObject = stateManager.getStateObject(dataname, ContentState.Type.tcpDownload);
						if(stateObject == null){
							System.out.println("StateObject is null");
							String portNum = AppConfig.getProperty("Custodian.Port");
							size = cacheStub.TCPRead(1,dataname,type,networkStack.getStackId()+":"+portNum);
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
	@Override
	public void upload(String data, int size) throws RemoteException {
		// TODO Auto-generated method stub
		
	}
}