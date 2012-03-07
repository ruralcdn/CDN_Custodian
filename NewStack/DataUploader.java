package NewStack;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import prototype.datastore.DataStore;
import newNetwork.Connection;
import newNetwork.ControlHeader;
import AbstractAppConfig.AppConfig;
import StateManagement.ContentState;
import StateManagement.StateManager;
import StateManagement.Status;

public class DataUploader extends Thread{

	private StateManager stateManager;
	private BlockingQueue<BlockingQueue<Packet>> emptyQueue;
	private Map<String,List<Connection>> connectionPool;
	private Map<String,ContentState>mpUp;
	private static boolean execute;
	private boolean pickTCPData;
	private Segmenter segmenter;
	LinkDetector ldetector;
	PolicyModule policyModule;
	DataStore store;

	public DataUploader(Segmenter seg,Map<String,List<Connection>> cp,LinkDetector detector,StateManager manager,BlockingQueue<BlockingQueue<Packet>> eQ,PolicyModule policy, DataStore dStore)
	{
		segmenter = seg;
		connectionPool = cp;
		stateManager = manager;
		emptyQueue = eQ;
		execute = true;
		ldetector = detector;
		policyModule = policy;
		pickTCPData = true;
		store = dStore ;
		mpUp = StateManager.getUpMap();
	}

	public void close()
	{
		execute = false;
	}

	@SuppressWarnings("deprecation")
	public void run()
	{
		while(execute)
		{
			try {
				
				BlockingQueue<Packet> packetQueue = emptyQueue.take(); 
				Set<String> destinationSet = connectionPool.keySet();  
				if(pickTCPData)
				{
					pickTCPData = false;
					List<String> tcpUploadData = stateManager.getTCPUploadRequests(); 
					int size = tcpUploadData.size(); 
					int count = 0;
					if(size == 0)
					{
						pickTCPData = false;
						emptyQueue.put(packetQueue);
						continue;
					}
					
					count++;
					String request = tcpUploadData.remove(0);
					ContentState stateObject = stateManager.getStateObject(request, ContentState.Type.tcpUpload);
					String destination = stateObject.getPreferredRoute().get(0);
					String[] conInfo = destination.split(":"); 
					ldetector.addDestination(conInfo[0]+":"+conInfo[1]); 
					while(!destinationSet.contains(conInfo[0]) && count <= size){
						tcpUploadData.add(tcpUploadData.size(),request);
						stateManager.setTCPUPloadRequestList(tcpUploadData);
						tcpUploadData = stateManager.getTCPUploadRequests();
						count++;
						request = tcpUploadData.remove(0);
						stateObject = stateManager.getStateObject(request, ContentState.Type.tcpUpload);
						destination = stateObject.getPreferredRoute().get(0);
						conInfo = destination.split(":"); 
						ldetector.addDestination(conInfo[0]+":"+conInfo[1]);
						System.out.println("Value of count: "+count);
					}

					if(count > size)
					{
						emptyQueue.put(packetQueue);
						tcpUploadData.add(tcpUploadData.size(),request);
						
					}	
					else
					{	
						ContentState downloadStateObject = stateManager.getStateObject(stateObject.getContentId(),ContentState.Type.tcpDownload);
						BitSet bitMap ;
						ControlHeader header = null;
						if(downloadStateObject == null){
							if(stateObject.getCurrentSegments()!=stateObject.getTotalSegments()){
								System.out.println("Download State Object is null");
								bitMap = null ;
								String finalDestination = stateObject.getPreferredRoute().get(0).split(":")[0];
								policyModule.setPolicy(finalDestination, Connection.Type.values()[stateObject.getPreferredInterface()]);
								header = new ControlHeader(stateObject.getAppId(),null,bitMap,stateObject.getOffset(),finalDestination,stateObject.getMetaDataFlag());
								segmenter.sendSegments(stateObject.getContentId(),stateObject.getUploadId(),header,  packetQueue,stateObject.getTotalSegments(),stateObject.getCurrentSegments() );
								tcpUploadData.add(tcpUploadData.size(),request);
							}
							else
							{
								Status st = Status.getStatus();
								String data = stateObject.getContentId();
								String fileType = st.getContentType("uploadrequest",data);
								store.rename(data, fileType);
								if(tcpUploadData.size()==0)
								{
									System.out.println("upload is finished");
									mpUp.remove(request);
									emptyQueue.put(packetQueue);
									execute = false ;
									stateManager.setTCPUPloadRequestList(tcpUploadData);
								    System.out.println("Execute = " + execute);
								    suspend();
								}
								else 
								{	
									System.out.println("Some upload is remained");
									mpUp.remove(request);
									emptyQueue.put(packetQueue);
									stateManager.setTCPUPloadRequestList(tcpUploadData);
								}	
							}
						}
						else{
							BitSet downMap = downloadStateObject.bitMap;
							BitSet upMap =stateObject.bitMap;
							int totalSeg = stateObject.getTotalSegments();
							int tcpSize = Integer.parseInt(AppConfig.getProperty("NetworkStack.SegmentSize"));
							int dtnSize = Integer.parseInt(AppConfig.getProperty("NetworkStack.dtnSegmentSize"));
							int scale = dtnSize/tcpSize ;
							bitMap = new BitSet(totalSeg);
							/*for(int i = 0 ; i < totalSeg ;i++){
								if(totalSeg == downloadStateObject.getTotalSegments()*scale){
									if((downMap.get(i)==false && upMap.get(i*scale)==false) ||(downMap.get(i)==true && upMap.get(i*scale)==true) ){
										for (int j= 0 ; j < scale ; j++){
											bitMap.set(i*scale+j);
										}
									}
										
								}
								else{
									if((downMap.get(i)==false && upMap.get(i)==false) ||(downMap.get(i)==true && upMap.get(i)==true) )
										bitMap.set(i);
								}	
							}*/
							if(scale == 1){
								for(int i = 0 ; i < totalSeg ;i++){
									if((downMap.get(i)==false && upMap.get(i)==false) ||(downMap.get(i)==true && upMap.get(i)==true) )
										bitMap.set(i);

								}
							}else{	
								for(int i = 0 ; i < totalSeg ;i=i+scale){
									//System.out.println("value of i: "+i);
									if((downMap.get(i/scale)==false && upMap.get(i)==false) ||(downMap.get(i/scale)==true && upMap.get(i)==true) ){
										for (int j= 0 ; j < scale ; j++){
											int seg = i+j;
											if(seg<totalSeg)
												bitMap.set(seg);
										}
									}
									
								}	
							}
							if(totalSeg !=stateObject.currentSegments)
							{
								String finalDestination = stateObject.getPreferredRoute().get(0).split(":")[0];
								policyModule.setPolicy(finalDestination, Connection.Type.values()[stateObject.getPreferredInterface()]);
								header = new ControlHeader(stateObject.getAppId(),null,bitMap,stateObject.getOffset(),finalDestination,stateObject.getMetaDataFlag());
								segmenter.sendPackets(stateObject.getContentId(),stateObject.getUploadId(),header,stateObject.getTotalSegments(), packetQueue, downloadStateObject.currentSegments, stateObject.currentSegments );
								tcpUploadData.add(tcpUploadData.size(),request);
								
							}
							else
							{
								Status st = Status.getStatus();
								String data = stateObject.getContentId();
								if(!(data.contains(".log") || data.contains(".jpg"))){
									String fileType = st.getContentType("uploadrequest",data);
									store.rename(data, fileType);
								}
								if(tcpUploadData.size()==0)
								{
									System.out.println("upload is finished");
									mpUp.remove(request);
									emptyQueue.put(packetQueue);
									execute = false ;
									stateManager.setTCPUPloadRequestList(tcpUploadData);
								    System.out.println("Execute = " + execute);
								    suspend();
								}
								else 
								{	
									System.out.println("Some upload is remained");
									mpUp.remove(request);
									emptyQueue.put(packetQueue);
									stateManager.setTCPUPloadRequestList(tcpUploadData);
								}	
							}
						}
					}
				}	
				else
				{
					boolean intersect = false;
					pickTCPData = true;
					List<String> dtnData = stateManager.getDTNData();
					int count = 0;
					int size = dtnData.size();
					if(dtnData.size() == 0)
					{
						pickTCPData = true;
						emptyQueue.put(packetQueue);
						continue;
					}
					String request = dtnData.remove(0);
					count++;
					ContentState stateObject = stateManager.getStateObject(request, ContentState.Type.dtn);
					List<String>  route = stateObject.getPreferredRoute();
					for(int i = 0;i < route.size();i++)
					{
						String nextHop = route.get(i).split(":")[0];
						if(destinationSet.contains(nextHop))
						{
							List<Connection> cons = connectionPool.get(nextHop);
							for(int j = 0;j < cons.size();j++)
							{
								if(cons.get(j).getType() == Connection.Type.USB)
									intersect = true;

							}

						}
					}
					while(!intersect && count <= size)
					{
						dtnData.add(dtnData.size(),request);
						stateManager.setDTNRequestList(dtnData);
						dtnData = stateManager.getDTNData();
						count++;
						request = dtnData.remove(0);
						stateObject = stateManager.getStateObject(request, ContentState.Type.dtn);
						route = stateObject.getPreferredRoute();
						for(int i = 0;i < route.size();i++)
						{
							String nextHop = route.get(i).split(":")[0];
							if(destinationSet.contains(nextHop))
							{
								List<Connection> cons = connectionPool.get(nextHop);
								for(int j = 0;i < cons.size();i++)
								{
									if(cons.get(j).getType() == Connection.Type.USB)
										intersect = true;

								}
							}
						}
					}

					if(count > size)
						emptyQueue.put(packetQueue);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}
	
	public boolean isNotRunning()
	{
		return execute ;
	}
	
	public void setExecute()
	{
		execute = true ;
	}

}