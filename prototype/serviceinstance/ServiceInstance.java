package prototype.serviceinstance;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import newNetwork.Connection;
import AbstractAppConfig.AppConfig;
import NewStack.NewStack;
import PubSubModule.IPubSubNode;
import PubSubModule.Notification;
import StateManagement.ContentState;
import StateManagement.ServiceInstanceAppStateManager;
import StateManagement.StateManager;

import prototype.dataserver.IDataServer;
import prototype.datastore.DataStore;
import prototype.userregistrar.ICustodianLogin;

@SuppressWarnings("unused")
public class ServiceInstance implements IUploader{

	DataStore store;
	StateManager stateManager;
	NewStack networkStack;
	IDataServer session;
	private static int contentId;
	String cacheId;
	String dataServerInfo;
	

	public ServiceInstance(String Id,DataStore dStore,StateManager manager,NewStack stack,IDataServer stub) 
	{
		cacheId = Id;
		store = dStore;
		networkStack = stack;
		session = stub;
		stateManager = manager;
		String dataServerId = AppConfig.getProperty("ServiceInstance.DataServer.IP");
		String dataServerPort = AppConfig.getProperty("ServiceInstance.DataServer.Port");
		String dataServerDTNId = AppConfig.getProperty("ServiceInstance.DataServer.DTNId");
		dataServerInfo = dataServerId+":"+dataServerPort+":"+dataServerDTNId;
	}

	public synchronized String generateContentId() throws RemoteException
	{
		contentId++;
		String rootServer = AppConfig.getProperty("ServiceInstance.Name");
		String newId = rootServer+"$"+Integer.toString(contentId);
		return newId;
	}

	public synchronized String generateContentId(String data) throws RemoteException
	{
		contentId++;
		String rootServer = AppConfig.getProperty("ServiceInstance.Name");
		String newId = rootServer+"$"+Integer.toString(contentId);
		return newId;
	}
	//called by data server to notify an upload which finally happened!
/*	public void notify_upload(String contentId) throws RemoteException
	{
		try
		{
			System.out.println("Upload to dataserver complete");

			String userContentId = SIAppManager.getDownloadName(contentId);
			String requester = SIAppManager.getUploadRequester(contentId);
			
			Registry serverRegistry;
			String userRegistrarIP = AppConfig.getProperty("ServiceInstance.UserRegistrar.IP");
			serverRegistry = LocateRegistry.getRegistry(userRegistrarIP);

			ICustodianLogin  userRegistrarStub = (ICustodianLogin) serverRegistry.lookup(AppConfig.getProperty("ServiceInstance.UserRegistrar.Service") );    //should be config driven
			String custodianId = userRegistrarStub.get_active_custodian(requester).split(":")[0];

			System.out.println("Active custodian is: "+custodianId);
			Registry custodianRegistry = LocateRegistry.getRegistry(custodianId);
			IPubSubNode pubsubStub = (IPubSubNode) custodianRegistry.lookup(AppConfig.getProperty("ServiceInstace.PubSub.Service") );
			Notification notification = new Notification(Notification.Type.UploadAck,requester+":"+userContentId);
			pubsubStub.notify(notification);
			//get information of the custodian
			//notify the custodian about the upload by the normal notify call(pubsub based)

		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}*/

	public String upload(String data,int size,String requester) throws RemoteException
	{
		
		System.out.println("Inside ServiceInstance.java: upload");
		String contentName = null;

		char[] bits = new char[size];
		for(int i = 0;i < size;i++)
		{
			bits[i] = '0';
		}
		//String bitMap = new String(bits);
		//maintain a state for the upload of the file
		contentName = generateContentId();
		System.out.println("ContentName= " + contentName);
		System.out.println("Size= " + size);
		System.out.println("Requester= " + requester);
		session.upload(contentName, size,requester,null);
		ContentState downloadStateObject = new ContentState(data,0,null,-1,null,size,0,ContentState.Type.tcpDownload,"1",true);
		stateManager.setStateObject(downloadStateObject);
		List<String> destinations = new ArrayList<String>();
		System.out.println("dataServerInfo : " + dataServerInfo);
		destinations.add(dataServerInfo);
		ContentState uploadStateObject = new ContentState(data,contentName,0,null,Connection.Type.DSL.ordinal(),
				destinations,size,0,ContentState.Type.tcpUpload,cacheId,true);
		stateManager.setTCPUploadState(uploadStateObject,null);

		return contentName;

	}

	public int processDynamicContent(String objectId,int uploadSize,String contentId,Connection.Type type,String requester,String conId) throws RemoteException
	{

		String sourceId = conId;			
		int segments = networkStack.countSegments("1$1.jpg");
		//as for now reply with a default content - 1$1.jpg
		
		if(type != Connection.Type.USB)
		{	

			char[] bits = new char[segments];
			for(int i =0;i < segments;i++)
			{
				bits[i] = '0';
			}

			String bitMap = new String(bits);
			List<String> destination = new ArrayList<String>();
			destination.add(sourceId);
			ContentState stateObject = new ContentState("1$1.jpg",contentId,0,null,type.ordinal(),destination,segments,0,ContentState.Type.tcpUpload,cacheId,true);
			stateManager.setTCPUploadState(stateObject,null);
			//SnetworkStack.setPolicy(1,type);
			//stack.sendSegments(1, contentId,contentId);
		}
		else
		{

			char[] bits = new char[segments];
			for(int i =0;i < segments;i++)
			{
				bits[i] = '1';
			}

			String bitMap = new String(bits);

			List<String> route = networkStack.getRoute(sourceId);
			ContentState stateObject = new ContentState("1$1.jpg",contentId,0,null,Connection.Type.USB.ordinal(),
					route,segments,0,ContentState.Type.dtn,cacheId,true);
			stateManager.setDTNState(stateObject);

		}
		return (int)segments;
	}

	public static void main(String[] args)  throws IOException{

		try {

			File configFile = new File("config/ServiceInstance.cfg");
			FileInputStream fis;
			fis = new FileInputStream(configFile);
			new AppConfig();
			AppConfig.load(fis);
			fis.close();

			//			Integer queueSize = Integer.parseInt(AppConfig.getProperty("ServiceInstance.Uploads.QueueSize"));		

			String dirPath = AppConfig.getProperty("ServiceInstance.Directory.Path");
			DataStore store = new DataStore(dirPath);

			File status = store.getFile(AppConfig.getProperty("ServiceInstance.StatusFile"));
			//StateManager stateManager = new StateManager(status,null);
			StateManager stateManager = new StateManager(null);

			//start TCP server
			int serverPort = Integer.parseInt(AppConfig.getProperty("ServiceInstance.DataConnection.Port"));
			
			DataStore usbStore = null;
			if(AppConfig.getProperty("Routing.allowDTN").equals("1"))
			{
				usbStore = new DataStore(AppConfig.getProperty("ServiceInstance.USBPath"));
			}

			String dataServer = AppConfig.getProperty("ServiceInstance.DataServer.IP");	
			String DataService = AppConfig.getProperty("ServiceInstance.DataServer.Service");
			Registry registry;

			registry = LocateRegistry.getRegistry(dataServer);
			String cacheId = AppConfig.getProperty("ServiceInstance.Id");
			IDataServer stub = (IDataServer) registry.lookup(DataService);       

			int maxCons = Integer.parseInt(AppConfig.getProperty("ServiceInstance.NetworkStack.MaximumConnections"));
			List<Integer> portList = new ArrayList<Integer>(maxCons);							//config driven
			int port = Integer.parseInt(AppConfig.getProperty("ServiceInstance.NetworkStack.Port"));
			for(int i = 0;i < 20;i++)
			{
				portList.add(port);
				port++;
			}

			NewStack stack = new NewStack(cacheId,stateManager,store,usbStore,null,serverPort,portList);
			
			String Id = AppConfig.getProperty("ServiceInstance.Id");

			ServiceInstance obj = new ServiceInstance(Id,store,stateManager,stack,stub);

			IUploader uploaderStub = (IUploader) UnicastRemoteObject.exportObject(obj, 0); 
			Registry registry1 = LocateRegistry.getRegistry();
			registry1.bind(AppConfig.getProperty("ServiceInstance.UploaderService"), uploaderStub);
			System.out.println("Service Instance ready");

		} catch (Exception e) {
			System.err.println("Client exception: " + e.toString());
			e.printStackTrace();
		}
	}

}
