package prototype.user;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.Date ;

import newNetwork.Connection;

import AbstractAppConfig.AppConfig;
import NewStack.NewStack;
import StateManagement.ApplicationStateManager;
import StateManagement.ContentState;
import StateManagement.StateManager;
import prototype.custodian.ICustodian;
import prototype.custodian.ICustodianSession;
import prototype.datastore.DataStore;

public class User1 extends UnicastRemoteObject implements IUser {
	
	
	private static final long serialVersionUID = 1L;
	DataStore store;   
	ICustodianSession session;
	StateManager stateManager;
	ApplicationStateManager appStateManager;
	File userConfiguration;
	String username;
	NewStack stack;
	AppFetcher fetcher;
	private static int AppId;
	String remoteHost;
	int remotePort;
	BlockingQueue<String> downloadList;
	String custodianId;
	Date d ;


	public User1(DataStore st) throws FileNotFoundException, IOException ,RemoteException
	{
		store = st;
		appStateManager = new ApplicationStateManager(store.getFile("status.cfg"));
		//stateManager = new StateManager(store.getFile("status.cfg"), "status");
		stateManager = new StateManager("status");
		AppId = 1; 
		
		
		DataStore usbStore = null;
		if(AppConfig.getProperty("Routing.allowDTN").equals("1"))
		{
			usbStore = new DataStore(AppConfig.getProperty("User.USBPath"));
		}
		
		
		
		int port = Integer.parseInt(AppConfig.getProperty("User.NetworkStack.Port"));
		List<Integer> portList = new ArrayList<Integer>(20);							
		for(int i = 0;i < 20;i++)
		{
			portList.add(port);
			port++;
		}
		
				
		String userId ;
		userId = AppConfig.getProperty("User.Id");
		stack = new NewStack(userId,stateManager,store,usbStore,downloadList,-1,portList);
		downloadList = new ArrayBlockingQueue<String>(Integer.parseInt(AppConfig.getProperty("User.MaximumDownloads")));
	}

	public void login(String username, String password) throws IOException, NotBoundException,RemoteException
	{
		
		System.out.println("Here In User1's login() method");
		this.username = username ;
		
		ICustodian stub;				
		custodianId = AppConfig.getProperty("User.Custodian.IP");
		System.out.println("User1 in "+custodianId );
		
		/***************RMI Registry with Custodian********************/
		Registry registry = LocateRegistry.getRegistry(custodianId);
		stub = (ICustodian) registry.lookup(AppConfig.getProperty("User.Custodian.Service") );   
		try
		{
			try
			{
				String userId = AppConfig.getProperty("User.Id");
				System.out.println("Calling authenticate in USER1.java In LogIn Method");
				session = stub.authenticate(username, password,userId,null);
			}catch(Exception e)
			{
				System.out.println("Calling register in USER1.java In LogIn Method");
				//stub.register(username, password);
				//stub.register_custodian(username);
				String userId = AppConfig.getProperty("User.Id");
				session = stub.authenticate(username, password,userId,null);
			}

			try
			{
				remoteHost = AppConfig.getProperty("User.Custodian.DataConnection.Server");
				remotePort = Integer.parseInt(AppConfig.getProperty("User.Custodian.DataConnection.Port"));
				stack.addDestination(remoteHost+":"+remotePort);
				System.out.println("User1.java In LogIn Method: Destination = "+remoteHost+":"+remotePort);
				fetcher = new AppFetcher(appStateManager,stateManager,session);
				//fetcher.start();

			}
			catch(Exception e)
			{
				e.printStackTrace();
				e.getCause();
			}

			//fetcher.start();

		}catch(Exception e)
		{
			e.printStackTrace();
		}


	}

	@SuppressWarnings("deprecation")
	public synchronized String generateContentId() throws RemoteException
	{
		AppId++ ; ///Changes made by Quamar
		d = new Date();
		String newId = username+"$"+(d.getMonth()+1)+d.getDate()+d.getHours()+d.getMinutes();
		return newId;
	}

	//service instance will be of format IP:port:destinationId
	public List<String> getUploadList() throws RemoteException ///Changes made by Quamar
	{
		List<String> uploadAckList = new ArrayList<String>();
		uploadAckList = appStateManager.getUploadAcks(AppId-1);//new ArrayList<String>();
		return uploadAckList;
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

	public List<String> getDownloadList(int AppId) throws RemoteException
	{
		List<String> AppIdDownloads = new ArrayList<String>();
		try
		{
			String[] downloads  = downloadList.toArray(new String[0]);
			for(int i = 0;i < downloads.length;i++)
			{
				String fileName = downloads[i];
				ContentState stateObject = stateManager.getStateObject(fileName,ContentState.Type.tcpDownload);
				//String requestedAppId = Integer.toString(AppId);
				//String AppIdOfDownload = stateObject.getAppId();
				if(stateObject.getAppId().equals(Integer.toString(AppId)))
				{
					AppIdDownloads.add(fileName);
				}
			}
			return AppIdDownloads;
		}catch(Exception e)
		{
			e.printStackTrace();
			return AppIdDownloads;
		}
	}

	public String upload(String data,Connection.Type type,int id,String serviceInstance) throws RemoteException
	{
		System.out.println("Inside USer1.java: upload");
		String contentName = null;
		String myContentName = null;
		if(store.contains(data))
		{
			myContentName = generateContentId();
			int segments = stack.countSegments(data);////Changes made by Quamar

			if(type != Connection.Type.USB)
			{
				try {
					
					contentName = session.upload( myContentName, segments,serviceInstance, myContentName, myContentName);
					
					char[] bits = new char[segments];
					for(int i =0;i < segments;i++)
					{
						bits[i] = '0';
					}

					//String bitMap = new String(bits);
					List<String> route = new ArrayList<String>();
					route.add(new String(remoteHost+":"+remotePort));
					ContentState stateObject = new ContentState(data,myContentName,0,null,
							Connection.Type.DSL.ordinal(),route,segments,0,ContentState.Type.tcpUpload,Integer.toString(id),true);
					stateManager.setTCPUploadState(stateObject,null);
					appStateManager.setServiceUploadName(myContentName, contentName);
					//	stack.setPolicy(id,type);
					//	stack.sendSegments(id,data,myContentName);


				} catch (Exception e) {
					e.printStackTrace();
					return contentName;
				}
			}
			else
			{
				char[] bits = new char[segments];
				for(int i =0;i < segments;i++)
				{
					bits[i] = '1';
				}

				List<String> route = null;
				//	List<String> route = stack.getRoute(serviceInstanceInfo);
				ContentState stateObject = new ContentState(data,myContentName,0,null,Connection.Type.USB.ordinal(),
						route,segments,0,ContentState.Type.dtn,Integer.toString(id),true);
				stateManager.setDTNState(stateObject);
				appStateManager.setServiceUploadName(myContentName, contentName);
				//stack.sendDTNSegments(id, data,destinationId);
			}
		}
		return contentName;
	}

	public String upload(String data,Connection.Type type,int id) throws RemoteException
	{

		System.out.println("User1.java: upload call from user to custodian ");
		String contentName = null;
		if(store.contains(data))
		{
			contentName = generateContentId();
			int segments = stack.countSegments(data);
			System.out.println("Segments = "+segments);

			if(type != Connection.Type.USB)
			{
				try {
					System.out.println("upload call from user to custodian");
					session.upload( contentName, segments);

					List<String> destinations = new ArrayList<String>();
					destinations.add(new String(remoteHost+":"+remotePort));
					char[] bits = new char[segments];
					for(int i =0;i < segments;i++)
					{
						bits[i] = '0';
					}

					//String bitMap = new String(bits);

					ContentState stateObject = new ContentState(data,contentName,0,null,
							Connection.Type.DSL.ordinal(),destinations,segments,0,ContentState.Type.tcpUpload,Integer.toString(id),true);
					stateManager.setTCPUploadState(stateObject,null);
					appStateManager.setServiceUploadName(contentName, contentName);
					//	stack.setPolicy(id,type);
					//	stack.sendSegments(id,data,contentName);


				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			else
			{
				String destination = null;
				destination = session.DTNUpload( data, segments,0, destination, destination, destination);
				char[] bits = new char[segments];
				for(int i =0;i < segments;i++)
				{
					bits[i] = '1';
				}

				//String bitMap = new String(bits);
				//find the destinations Id
				//find the destinations Id
				//update it 
				//this will not work for 
				List<String> route = stack.getRoute(destination);
				ContentState stateObject = new ContentState(data,contentName,0,null,Connection.Type.USB.ordinal(),
						route,segments,0,ContentState.Type.dtn,Integer.toString(id),true);
				stateManager.setDTNState(stateObject);
				appStateManager.setServiceUploadName(contentName, contentName);
				//	stack.sendDTNSegments(id, data,destinationId);

			}
		}
		return contentName;
	}

	public String processDynamicContent(int id,String contentId,Connection.Type uploadType,Connection.Type downloadType,String dest) throws RemoteException
	{
		String uploadContentName = null;
		String downloadContentName = null;
		if(store.contains(contentId))
		{
			uploadContentName = generateContentId();
			downloadContentName = generateContentId();
			int segments = stack.countSegments(contentId);
			List<String> reply;
			reply = session.processDynamicContent(uploadContentName,segments, downloadContentName, uploadType, downloadType, dest); 
			if(reply != null)
			{
				int size = Integer.parseInt(reply.get(0));
				String serviceInstanceInfo = reply.get(1);
				if(uploadType != Connection.Type.USB)
				{
					try {
						char[] bits = new char[segments];
						for(int i =0;i < segments;i++)
						{
							bits[i] = '0';
						}
						//String bitMap = new String(bits);
						List<String> destinations = new ArrayList<String>();
						destinations.add(new String(remoteHost+":"+remotePort));
						ContentState stateObject = new ContentState(contentId,uploadContentName,0,null,
								uploadType.ordinal(),destinations,segments,0,ContentState.Type.tcpUpload,Integer.toString(id),true);
						stateManager.setTCPUploadState(stateObject,null);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				else
				{

					char[] bits = new char[segments];
					for(int i =0;i < segments;i++)
					{
						bits[i] = '1';
					}
					//String bitMap = new String(bits);
					List<String> route = stack.getRoute(serviceInstanceInfo);
					ContentState stateObject = new ContentState(contentId,uploadContentName,0,null,Connection.Type.USB.ordinal(),
							route,segments,0,ContentState.Type.dtn,Integer.toString(id),true);
					stateManager.setDTNState(stateObject);
				}

				try
				{
					/*
					 * the connectionType sent in the find request should be later
					 * decided by a module like a policy module to decide on the connection
					 */


						//String bitMap = new String("");

						if(size > 0)
						{
							char[] bits = new char[size];
							for(int i =0;i < size;i++)
							{
								bits[i] = '0';
							}
							//bitMap = new String(bits);

						}	
						List<String> destinations = new ArrayList<String>();
						destinations.add(new String(remoteHost+":"+remotePort));
						ContentState downloadStateObject = new ContentState(downloadContentName,0,null,
								downloadType.ordinal(),destinations,size,0,ContentState.Type.tcpDownload,Integer.toString(id),true);
						if(downloadType != Connection.Type.USB)
						{
						stateManager.setTCPDownloadState(downloadStateObject);
						}
					else
					{
						stateManager.setStateObject(downloadStateObject);
					}
				}catch(Exception e)
				{
					e.printStackTrace();
				}
			}
		}

		return downloadContentName;

	}

	public void find(String data,Connection.Type type,int id) throws RemoteException
	{

		try
		{
			/**
			 * the connectionType sent in the find request should be later
			 * decided by a module like a policy module to decide on the connection
			 */
			InetAddress host = InetAddress.getLocalHost();
			String ip = host.getHostAddress();
			//int size = (int) session.find(id,data,type,ip);
			int size = (int) session.find(id,data,type,ip,0);
			if(type != Connection.Type.USB)
			{
				//String bitMap = new String("");

				if(size > 0)
				{
					char[] bits = new char[size];
					for(int i =0;i < size;i++)
					{
						bits[i] = '0';
					}
					//bitMap = new String(bits);

				}	

				List<String> destinations = new ArrayList<String>();
				destinations.add(new String(remoteHost+":"+remotePort));
				ContentState stateObject = new ContentState(data,0,null,type.ordinal(),destinations,size,0,ContentState.Type.tcpDownload,Integer.toString(id),true);
				stateManager.setTCPDownloadState(stateObject);

			}
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	//Method added by Quamar
	public int getAppId()throws RemoteException{
		return AppId ;
	}
	
	public void uploadStatus(String name) throws RemoteException{
		stateManager.uploadStat(name);
		
	}

	public void logout() throws RemoteException
	{
		session.close_connection();  
		stack.close();
		//	fetcher.close();
		//	uploader.close();
	}
	
	
	
	
	

	public static void main(String[] args) throws Exception{

		File configFile = new File("config/User.cfg");
		FileInputStream fis;
		fis = new FileInputStream(configFile);
		new AppConfig();
		AppConfig.load(fis);
		fis.close();

		DataStore store = new DataStore(AppConfig.getProperty("User.Directory.path"));
				
		/**************Start the RMI Service named "user daemon" *****************************/
		User1 obj = new User1(store);
		Naming.bind(AppConfig.getProperty("User.Service"), obj);

		System.err.println("Class User1: Server ready");

	}
}
