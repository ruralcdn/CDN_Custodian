package prototype.dataserver;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.concurrent.BlockingQueue;
import PubSubModule.IPubSubNode;
import PubSubModule.Notification;
import StateManagement.ServiceInstanceAppStateManager;
import prototype.rootserver.IRootServer;
import prototype.userregistrar.ICustodianLogin;
import AbstractAppConfig.AppConfig;

public class FileRegisterer extends Thread{
	
	BlockingQueue<String> files;
	boolean execute;
	ServiceInstanceAppStateManager AppManager;
	
	public FileRegisterer(BlockingQueue<String> fileDownloads,ServiceInstanceAppStateManager appManager)
	{
		files = fileDownloads;
		AppManager = appManager;
		execute = true;
	}
	
	public void close()
	{
		execute = false;
	}
	
	public void run()
	{
		while(execute)
		{
			try {
				
				String newFile = files.take();
				System.out.println("Requesting root server:"+AppConfig.getProperty("DataServer.RootServer.Service"));
				Registry registry = LocateRegistry.getRegistry(AppConfig.getProperty("DataServer.RootServer.IP"));
				IRootServer stub = (IRootServer) registry.lookup(AppConfig.getProperty("DataServer.RootServer.Service") );
				String location = AppConfig.getProperty("DataServer.Id")+":"+AppConfig.getProperty("DataServer.Port")+":"+AppConfig.getProperty("DataServer.Id");
				stub.register(newFile,location);                              //should be config driven
				
				
				try
				{
					System.out.println("Upload to dataserver complete");

					//String userContentId = AppManager.getDownloadName(newFile);
					String requester = AppManager.getUploadRequester(newFile);
					
					Registry serverRegistry;
					String userRegistrarIP = AppConfig.getProperty("UserRegistrar.IP");
					serverRegistry = LocateRegistry.getRegistry(userRegistrarIP);

					ICustodianLogin  userRegistrarStub = (ICustodianLogin) serverRegistry.lookup(AppConfig.getProperty("UserRegistrar.Service") );    //should be config driven
					String custodianId = userRegistrarStub.get_active_custodian(requester).split(":")[0];

					System.out.println("Active custodian is: "+custodianId);
					Registry custodianRegistry = LocateRegistry.getRegistry(custodianId);
					IPubSubNode pubsubStub = (IPubSubNode) custodianRegistry.lookup(AppConfig.getProperty("PubSub.Service") );
					Notification notification = new Notification(Notification.Type.UploadAck,requester+":"+newFile);
					pubsubStub.notify(notification);
					//get information of the custodian
					//notify the custodian about the upload by the normal notify call(pubsub based)

				}catch(Exception e)
				{
					e.printStackTrace();
				}
				
				
			/*	Registry serverRegistry = LocateRegistry.getRegistry(AppConfig.getProperty("DataServer.ServiceInstace.IP"));
				IUploader serverStub = (IUploader) serverRegistry.lookup(AppConfig.getProperty("DataServer.ServiceInstace.Service") );
				serverStub.notify_upload(newFile);
			*/	
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}	
	
}