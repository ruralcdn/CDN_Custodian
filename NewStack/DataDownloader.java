package NewStack;

import java.util.Iterator;
import java.util.List;

import StateManagement.ContentState;
import StateManagement.StateManager;;


public class DataDownloader extends Thread{
	
	StateManager stateManager;
	boolean execute;
	String localId;
	LinkDetector ldetector;
	
	public DataDownloader(String Id,StateManager manager,LinkDetector detector)
	{
		localId = Id;
		stateManager = manager;
		ldetector = detector;
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
				List<String> requestedData = stateManager.getTCPDownloadRequests();
				Iterator<String> it = requestedData.iterator();
				while(it.hasNext())
				{
					String data = it.next();
					it.remove();
					ContentState stateObject = stateManager.getStateObject(data, ContentState.Type.tcpDownload);
					List<String> caches = stateObject.getPreferredRoute();
					if(!caches.isEmpty())
					{
						int totalSegments = stateObject.getTotalSegments();
						int currentSegments = stateObject.currentSegments;						
						
						if(currentSegments != totalSegments)
						{
							String cache = caches.get(0);
							String[] cacheInformation = cache.split(":");
							String Id = cacheInformation[0];
							int port = Integer.parseInt(cacheInformation[1]);
							System.out.println("In DataDownloader.java IP is : "+Id);
							System.out.println("Requesting Data thru DataDownloader.java");
							ldetector.addDestination(Id+":"+port);
							//stub.request_data(localId,data,offset,type,sendMetaDataFlag,totalSegments,currentSegments);

						}
						
					}	
				}
				
				/*try
				{
					Thread.sleep(500);
				}catch(InterruptedException e)
				{
					e.printStackTrace();
				}*/

			}catch(Exception e)
			{
				e.printStackTrace();
			}

		}
	}

}
