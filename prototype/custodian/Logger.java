package prototype.custodian;

import java.io.File;
import java.io.FileInputStream;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import AbstractAppConfig.AppConfig;
import NewStack.NewStack;
import PubSubModule.IPubSubNode;
import PubSubModule.PubSubNode;
import StateManagement.ContentState;
import StateManagement.CustodianAppStateManager;
import StateManagement.StateManager;
import StateManagement.Status;
import prototype.custodian.FileRegisterer;
import prototype.datastore.DataStore;
import prototype.userregistrar.IUserRegistrarSession;
import prototype.userregistrar.ICustodianLogin;
import prototype.utils.AuthenticationFailedException;
import prototype.utils.NotRegisteredException;

public class Logger implements ICustodian
{

							
	ICustodianLogin stub;                    						
	DataStore store;											
	NewStack networkStack;									
	StateManager stateManager;							    
	CustodianAppStateManager appStateManager;
	BlockingQueue<String> fileList ;
	
	static List<String> readingList ;
	static Map<String,List<String>> writingListMap;
	
	static ArrayList<String> uploadSyncList;
	public Logger(ICustodianLogin loginStub,StateManager sManager,CustodianAppStateManager caManager/*BlockingQueue<Map<String,List<String>>> datarequests*/,
			DataStore st,NewStack stack,BlockingQueue<String> fileDownloads, String custodianId)
	{
		stub = loginStub;
		stateManager = sManager;
		appStateManager = caManager;
		store = st;
		networkStack = stack;
		fileList = fileDownloads ;
		FileRegisterer registerer = new FileRegisterer(fileDownloads,custodianId);
		registerer.start();
	}
	
	public ICustodianSession authenticate(String username,String password, String userNode, String userControlIP) throws RemoteException,NotRegisteredException,AuthenticationFailedException
	{
		System.out.println("Inside Logger.java:authenticate");
		IUserRegistrarSession sessionStub = null;  
		try
		{
			synchronized(stub)
			{
				sessionStub = stub.authenticate_user(username, password,networkStack.getStackId()+":"+networkStack.getServerPort()+":"+networkStack.getDTNId());
				Status st = Status.getStatus();
				// added by Arvind
				this.infoIP(userNode, userControlIP);  // it makes entry for (user,IPaddr) in userdaemonloc
				String userNod  = st.updateUserLocation("select * from userlocation where username = '"+username+"'", 2); 
				if(userNod.length() == 0)
					st.updateUserLocation("insert into userlocation values ('"+username+"','"+userNode+"')",1);
				else
					st.updateUserLocation("update userlocation set userNode ='"+ userNode+"' where username ='"+username+"'", 1);
			}
		}
		catch(AuthenticationFailedException e)
		{
			System.out.println("Authentication invalidated");
			
		}
		catch(ConnectException e)
		{
			
			String RegistrarServer = AppConfig.getProperty("Custodian.UserRegistrar.IP");    
			Registry registrarRegistry = LocateRegistry.getRegistry(RegistrarServer);
			String userRegistrarService = AppConfig.getProperty("Custodian.UserRegistrar.Service");
			System.out.println("Finding userRegistrarService.");
			
			boolean bound = false;
			while(!bound)
			{
				try
				{
					stub = (ICustodianLogin) registrarRegistry.lookup(userRegistrarService);
					bound = true;
				}
				catch(Exception ex)
				{
					ex.printStackTrace();
					try {
						
						Thread.sleep(1000);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
				}
			}
			System.out.println("Found userRegistrarService.");
			synchronized(stub)
			{
				sessionStub = stub.authenticate_user(username, password,networkStack.getStackId()+":"+networkStack.getServerPort()+":"+networkStack.getDTNId());
			}
			
		}
		ICustodianSession session;
		if(sessionStub != null)
		{
			//session = new CustodianSession(username,sessionStub,stateManager,appStateManager,store,networkStack);
			session = new CustodianSession(username,sessionStub,stateManager,appStateManager,store,networkStack, readingList, writingListMap, uploadSyncList);
			ICustodianSession custodianSessionStub = (ICustodianSession) UnicastRemoteObject.exportObject(session, 0);
			System.out.println("User authenticated,beginning session");
			return custodianSessionStub;
		}
		else
		   return null ;//throw new AuthenticationFailedException();
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
		System.out.print(pendingPackets.size()+" Pending packets for: "+contentName+" : ");
		for (int i = 0; i < pendingPackets.size(); i++)
			System.out.print(pendingPackets.get(i)+" ");
		System.out.println();
		return pendingPackets ;
	}
	
	public void infoIP(String userId, String IPAdd) throws RemoteException{
		Status st = Status.getStatus();
		st.updateUserDaemonLocation(IPAdd,userId);
		
	}
	public static void main(String args[]) {

		try {
			File configFile = new File("config/Custodian.cfg");
			FileInputStream fis;
			fis = new FileInputStream(configFile);
			new AppConfig();
			AppConfig.load(fis);
			fis.close();
			
			DataStore store = new DataStore(AppConfig.getProperty("Custodian.Directory.Path"));
			//StateManager stateManager = new StateManager("statusData");
			StateManager stateManager = new StateManager("status");
			CustodianAppStateManager appStateManager = new CustodianAppStateManager();
			
			PubSubNode node = new PubSubNode(appStateManager,null);
			System.getProperties().setProperty("java.rmi.server.hostname","mycustodian");
			
			IPubSubNode pubsubStub = (IPubSubNode) UnicastRemoteObject.exportObject(node, 0);
			Registry pubsubRegistry = LocateRegistry.getRegistry();
			
			
			boolean found = false;
			
			while(!found)
			{
				try
				{
					pubsubRegistry.bind(AppConfig.getProperty("Custodian.PubSub.Service") , pubsubStub);
					found = true;
				}
				catch(AlreadyBoundException ex)
				{
					pubsubRegistry.unbind(AppConfig.getProperty("Custodian.PubSub.Service"));
					pubsubRegistry.bind(AppConfig.getProperty("Custodian.PubSub.Service") , pubsubStub);
					found = true;
				}
				catch(ConnectException ex)
				{
					String rmiPath = AppConfig.getProperty("Custodian.Directory.rmiregistry");
					Runtime.getRuntime().exec(rmiPath);
					//Runtime.getRuntime().exec("C:\\Program Files\\Java\\jdk1.6.0_16\\bin\\rmiregistry.exe");
				}
			}	
			
			int port = Integer.parseInt(AppConfig.getProperty("Custodian.Port"));
			String custodianId = AppConfig.getProperty("Custodian.Id");
			DataStore usbStore = null;
			if(AppConfig.getProperty("Routing.allowDTN").equals("1"))
			{
				usbStore = new DataStore(AppConfig.getProperty("Custodian.USBPath"));
			}
			
			int clientPort = Integer.parseInt(AppConfig.getProperty("Custodian.NetworkStack.Port"));
			int tempPort = clientPort;
			List<Integer> cacheConnectionPorts = new ArrayList<Integer>(Integer.parseInt(AppConfig.getProperty("Custodian.ConnectionPool.Size")));     //config driven
			for(int i = 0; i < 20;i++)
			{
				cacheConnectionPorts.add(tempPort);
				tempPort++;
			}
			
			int maxDownloads = 40;//Integer.parseInt(AppConfig.getProperty("CacheServer.MaximumDownloads"));
			BlockingQueue<String> fileDownloads = new ArrayBlockingQueue<String>(maxDownloads);   
			//FileRegisterer registerer = new FileRegisterer(fileDownloads,AppManager);
			readingList = new ArrayList<String>();
			writingListMap = new HashMap<String, List<String>>();
			uploadSyncList = new ArrayList<String>();
			NewStack stack = new NewStack(custodianId,stateManager,store,usbStore,fileDownloads,port,cacheConnectionPorts,readingList,writingListMap);

			String RegistrarServer = AppConfig.getProperty("Custodian.UserRegistrar.IP");    
			Registry registrarRegistry = LocateRegistry.getRegistry(RegistrarServer);
			String userRegistrarService = AppConfig.getProperty("Custodian.UserRegistrar.Service");
			
			ICustodianLogin registrarStub = null;
			System.out.println("Finding userRegistrarService.");
			
			boolean bound = false;
			while(!bound)
			{
				try
				{
					registrarStub = (ICustodianLogin) registrarRegistry.lookup(userRegistrarService);
					bound = true;
				}
				catch(Exception ex)
				{
					//ex.printStackTrace();
					Thread.sleep(1000);
				}
			}
			System.out.println("UserRegistrarService Found.");		

			//System.getProperties().setProperty("java.rmi.server.hostname","mycustodian");
			Logger obj = new Logger(registrarStub,stateManager,appStateManager,store,stack, fileDownloads,custodianId);
			ICustodian custodianStub = (ICustodian) UnicastRemoteObject.exportObject(obj, 0);
			
			Registry registry = LocateRegistry.getRegistry();
			boolean foundCustodianService = false;
			while(!foundCustodianService)
			{
				try
				{
					registry.bind(AppConfig.getProperty("Custodian.Service") , custodianStub);
					foundCustodianService = true;
				}
				catch(AlreadyBoundException ex)
				{
					//ex.printStackTrace();
					registry.unbind(AppConfig.getProperty("Custodian.Service"));
					registry.bind(AppConfig.getProperty("Custodian.Service") , custodianStub);
					foundCustodianService = true;
				}
				catch(ConnectException ex)
				{
					//ex.printStackTrace();
					foundCustodianService = false;
					String rmiPath = AppConfig.getProperty("Custodian.Directory.rmiregistry");
					//Runtime.getRuntime().exec("C:\\Program Files\\Java\\jdk1.6.0_16\\bin\\rmiregistry.exe");
					Runtime.getRuntime().exec(rmiPath);
				}
				
			}

			System.err.println("Server ready");
		} catch (Exception e) {
			System.err.println("Server exception: " + e.toString());
			e.printStackTrace();
		}
	}


}