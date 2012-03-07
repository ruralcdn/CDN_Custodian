package prototype.cache;

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
		upSync = CacheServer.UploadSyncList;
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
				System.out.println("Here in FileRegisterer");
				String newFile = files.take();
				Registry registry = LocateRegistry.getRegistry(AppConfig.getProperty("CacheServer.RootServer.IP"));
				IRootServer stub = (IRootServer) registry.lookup(AppConfig.getProperty("CacheServer.RootServer.Service") );
				String location = AppConfig.getProperty("CacheServer.Id")+":"+AppConfig.getProperty("CacheServer.Port")+":"+AppConfig.getProperty("CacheServer.Id");
				stub.register(newFile,location); 
				/*if(upSync.contains(newFile))
				{
					//int count = Integer.parseInt(newFile.substring(newFile.indexOf('$')+1,newFile.lastIndexOf('.')));
					System.out.println("Here in FileRegisterer");
					//String str = "insert into dirtable values("+count+",'cfile','"+newFile+"')";
					String str = "insert into dirtable values('"+cacheId+"','"+newFile+"')";
					insmp.put(str);
				}*/
				
			} catch (Exception e) 
			{
				e.printStackTrace();
			}
		}
		
	}	
	
}