package prototype.dbserver;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import StateManagement.ServiceInstanceAppStateManager;
import StateManagement.Status;
import prototype.user.IAppFetcher;

public class LogRegisterer extends Thread{
	
	BlockingQueue<String> files;
	boolean execute;
	ServiceInstanceAppStateManager AppManager;
	List<String> upSync ;
	String cacheId ;
	public LogRegisterer(BlockingQueue<String> fileDownloads/*,ServiceInstanceAppStateManager* appManager*/)
	{
		files = fileDownloads;
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
			try 
			{
				
				String newFile = files.take();
				if(newFile.contains(".log")){
					String usernode = newFile.substring(0,newFile.indexOf('.'));
					System.out.println("usernode location is: "+usernode);
					boolean flag = false ;
					while(!flag){
						try
						{
							Status st = Status.getStatus();
							String userAdd = st.getUserDaemonLocation(usernode);
							Registry userRegistry = LocateRegistry.getRegistry(userAdd);
							IAppFetcher appFetcherStub = (IAppFetcher) userRegistry.lookup("appfetcher");
							appFetcherStub.uploadLog();
							flag = true ;

						}catch(Exception e){
							e.printStackTrace();
							try {
								Thread.sleep(1000);
							} catch (Exception e1) {

							}
						}
					}
				}
				
			} catch (Exception e) 
			{
				e.printStackTrace();
			}
		}
		
	}	
	
}