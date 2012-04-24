package NewStack;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import newNetwork.Connection;
import prototype.datastore.DataStore;
import prototype.user.IUser;
import AbstractAppConfig.AppConfig;
import StateManagement.ContentState;
import StateManagement.StateManager;
import StateManagement.Status;

public class NewStack
{
	private String stackId;
	DTNReader dtnReader;
	DTNWriter dtnWriter;
	private StateManager stateManager;
	private DataStore store;
	private LinkDetector detector;
	private int segmentSize = Integer.parseInt(AppConfig.getProperty("NetworkStack.SegmentSize"));
	private int dtnsegmentSize = Integer.parseInt(AppConfig.getProperty("NetworkStack.dtnSegmentSize"));
	private Scheduler scheduler;
	private PolicyModule policyModule;
	private TCPServer server;
	private SegmentationReassembly sar;
	private static DataUploader uploader;
	private DataDownloader downloader;
	private static RMIServer controlServer;
	Reassembler reassembler;
	
	public NewStack(String localId,StateManager manager,DataStore dStore,DataStore usbStore,BlockingQueue<String> downloads,int serverPort,List<Integer> connectionPorts)
	{
		stackId = localId;
		stateManager = manager;
		store = dStore;
		policyModule = new PolicyModule();
		scheduler = new Scheduler(policyModule,stateManager,segmentSize);
		sar = new SegmentationReassembly(stateManager,store,scheduler,segmentSize,downloads);
		detector = new LinkDetector(stackId,scheduler,connectionPorts,dStore,usbStore);
		if(serverPort != -1)
		{
			server = new TCPServer(scheduler,serverPort);
			new Thread(server).start();
			controlServer = new RMIServer(stateManager,store,sar,policyModule,scheduler.getDataEmptyQueues(),detector);
			
		}
		uploader = new DataUploader(sar.getSegmenter(),scheduler.getConnectionPool(),detector,stateManager,scheduler.getDataEmptyQueues(),policyModule,dStore);
		int size = stateManager.getTCPUploadRequests().size();
		if(size >= 1)
		{	
			System.out.println("Starting the Uploader with size: "+size);
			uploader.start();
		}	
		if(stateManager.getTCPDownloadRequests().size()>=1){
			controlServer.start();
			System.out.println("Starting the RMI server");
		}	
	}
	
	public NewStack(String localId,StateManager manager,DataStore dStore,DataStore usbStore,BlockingQueue<String> downloads,int serverPort,List<Integer> connectionPorts, List<String> readList, Map<String,List<String>> writeListMap)
	{
		stackId = localId;
		stateManager = manager;
		store = dStore;
		policyModule = new PolicyModule();
		scheduler = new Scheduler(policyModule,stateManager,segmentSize);
		sar = new SegmentationReassembly(stateManager,store,scheduler,segmentSize,downloads);
		detector = new LinkDetector(stackId,scheduler,connectionPorts,dStore,usbStore);
		if(serverPort != -1)
		{
			server = new TCPServer(scheduler,serverPort);
			new Thread(server).start();
			controlServer = new RMIServer(stateManager,store,sar,policyModule,scheduler.getDataEmptyQueues(),detector);
			
		}
		uploader = new DataUploader(sar.getSegmenter(),scheduler.getConnectionPool(),detector,stateManager,scheduler.getDataEmptyQueues(),policyModule,dStore);
		int size = stateManager.getTCPUploadRequests().size();
		if(size != 0)
		{	
			System.out.println("Starting the Uploader with size: "+size);
			uploader.start();
		}	
		if(stateManager.getTCPDownloadRequests().size()!= 0)
		{
			controlServer.start();
			List<String> downloadRequest = new ArrayList<String>();
			Map<String,List<ContentState>> contUpMultiMap = new HashMap<String,List<ContentState>>();
			contUpMultiMap = stateManager.getcontUpMultiMap();
			downloadRequest = stateManager.getTCPDownloadRequests();
			//System.out.println("Size of download request"+downloadRequest.size());
			if(downloadRequest.size()!= 0)
			{
				for(int i = 0 ; i < downloadRequest.size() ;i++){
					String content = downloadRequest.get(i);
					List<ContentState> lists = new ArrayList<ContentState>();
					lists = contUpMultiMap.get(content);
					//System.out.println("Size of lists: "+lists);
					for(int j = 0 ; j < lists.size() ;j++){
						ContentState stateObject = lists.get(i);
						String destination = stateObject.getPreferredRoute().get(0);
						String[] destinationSplit = destination.split(":");
						IUser stub = null ;
						Registry registry = null;
						String contentName = stateObject.getUploadId();
						System.out.println("In StateManager, Content Name is: "+contentName);
						boolean bound = false;
						while(!bound)
						{
							try
							{
								Status st = Status.getStatus();
								String usernode = st.updateUserLocation("select distinct(ipadd) from userlocation where userNode = '"+destinationSplit[0]+"'", 2);
								registry = LocateRegistry.getRegistry(usernode);
								stub = (IUser) registry.lookup("userdaemon");
								stub.getUploadAcks(contentName, stateObject.getTotalSegments());
								bound = true;						
							}
							catch(Exception ex)
							{
								try {
									Thread.sleep(1000);
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
								System.out.println("Error in contacting UserDaemon");
							}
						}
						
					}
				}
			}
			
			System.out.println("Starting the RMI server");
		}	
		//dtnReader = new DTNReader(readList, stateManager,store,dtnsegmentSize,downloads);
		//dtnReader.start();	
		
		dtnWriter = new DTNWriter(readList,writeListMap,stateManager,store,dtnsegmentSize,downloads);
		dtnWriter.start();
	}

	public NewStack(String localId,StateManager manager,DataStore dStore,BlockingQueue<String> downloads)
	{
		stackId = localId;
		stateManager = manager;
		store = dStore;
		policyModule = new PolicyModule();
		scheduler = new Scheduler(policyModule,stateManager,segmentSize);
		sar = new SegmentationReassembly(stateManager,store,scheduler,segmentSize,downloads);
		uploader = new DataUploader(sar.getSegmenter(),scheduler.getConnectionPool(),null,stateManager,scheduler.getDataEmptyQueues(),policyModule,dStore);
		uploader.start();
	}

	public void addDestination(String destination)
	{
		detector.addDestination(destination);
	}

	public int countSegments(String contentname)
	{
		return (int)sar.countSegments(contentname);
	}

	public int countdtnSegments(String contentname)
	{
		return (int)sar.countdtnSegments(contentname);
	}
	public void close()
	{
		detector.close();
		scheduler.close();
		server.close();
		sar.close();
		uploader.close();
		downloader.close();
	}

	public void addConnection(String remoteId,Connection con)
	{
		scheduler.addConnection(remoteId, con);
	}
	
	public List<String> getRoute(String destination)
	{
		return RouteFinder.findDTNRoute(destination);
	}
	
	public void setPolicy(String Id,Connection.Type type)
	{
		policyModule.setPolicy(Id, type);
	}
	public String getStackId()
	{
		return stackId;
	}
	public int getServerPort()
	{
		return server.getServerPort(); 
	}
	public String getDTNId()
	{
		File configFile = store.getFile(AppConfig.getProperty("DTN.Router.ConfigFile"));
		Properties Config = new Properties();
		FileInputStream fis;
		try {
			fis = new FileInputStream(configFile);
			Config.load(fis);
			fis.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}catch (IOException e) {
			e.printStackTrace();
		} 
		return Config.getProperty("DTNId");
	}
	
	public static DataUploader getDataUploader()
	{
		return uploader ;
	}
	public static RMIServer getRMIServer()
	{
		return  controlServer;
	}
}