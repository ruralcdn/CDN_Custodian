package prototype.custodian;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import StateManagement.ServiceInstanceAppStateManager;
import prototype.rootserver.IRootServer;
import AbstractAppConfig.AppConfig;

public class FileRegisterer extends Thread{
	
	BlockingQueue<String> files;
	boolean execute;
	ServiceInstanceAppStateManager AppManager;
	List<String> upSync ;
	//BlockingQueue<String> insmp ;
	String cacheId ;
	//public FileRegisterer(BlockingQueue<String> fileDownloads,ServiceInstanceAppStateManager appManager)
	public FileRegisterer(BlockingQueue<String> fileDownloads/*,ServiceInstanceAppStateManager* appManager*/, String id)
	{
		files = fileDownloads;
		execute = true;
		upSync = Logger.uploadSyncList;
		//insmp = LocalUpdate.insMap;
		cacheId = id ;
	}
	
	public void close()
	{
		execute = false;
	}
	
	public void run()
	{
		while(execute)
		{
			try 
			{
				
				String newFile = files.take();
				Registry registry = LocateRegistry.getRegistry(AppConfig.getProperty("Custodian.RootServer.IP"));
				IRootServer stub = (IRootServer) registry.lookup(AppConfig.getProperty("Custodian.RootServer.Service") );
				String location = AppConfig.getProperty("Custodian.Id")+":"+AppConfig.getProperty("Custodian.Port")+":"+AppConfig.getProperty("Custodian.Id");
				stub.register(newFile,location); 
				if(upSync.contains(newFile))
				{
					@SuppressWarnings("unused")
					String str = "insert into dirtable values('"+cacheId+"','"+newFile+"')";
					//insmp.put(str);
				}
				
			} catch (Exception e) 
			{
				e.printStackTrace();
			}
		}
		
	}	
	
}