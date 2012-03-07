/*package NewStack;

import java.rmi.RemoteException;
import java.util.concurrent.BlockingQueue;
import newNetwork.Connection;
import newNetwork.ControlHeader;
import prototype.datastore.DataStore;

public class RMIServer implements IRMIServer{

	private DataStore store;
	private SegmentationReassembly sar;
	private PolicyModule policyModule;
	private BlockingQueue<BlockingQueue<Packet>> emptyQueue;

	public RMIServer(DataStore dStore,SegmentationReassembly sr,PolicyModule policy,BlockingQueue<BlockingQueue<Packet>> eQ)
	{
		store = dStore;
		sar = sr;
		policyModule = policy;
		emptyQueue = eQ;
	}

	public int request_data(String requesterId,String data,int offset,Connection.Type type,boolean sendMetaData,int totSeg,int curSeg) throws RemoteException
	{
		if(store.contains(data) && store.contains(data+".marker") && type != Connection.Type.USB)
		{
			BlockingQueue<Packet> packetQueue = emptyQueue.poll();
			if(packetQueue != null)
			{
				String bitMap = null;
				Segmenter segmenter = sar.getSegmenter();
				ControlHeader header = new ControlHeader(requesterId,null,bitMap,offset,requesterId,sendMetaData);
				segmenter.sendSegments(data, data, header, packetQueue,totSeg,curSeg);
				policyModule.setPolicy(requesterId, type);
				return (int) sar.countSegments(data);
			}
			else
				return -1;
		}
		else
			return -1;
	}
}*/

package NewStack;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import StateManagement.ContentState;
import StateManagement.StateManager;
import StateManagement.Status;
import newNetwork.Connection;
import newNetwork.ControlHeader;
import prototype.datastore.DataStore;
import prototype.user.IUser;

public class RMIServer extends Thread implements IRMIServer{

	private StateManager stateManager;
	private LinkDetector ldetector ;
	private SegmentationReassembly sar;
	private PolicyModule policyModule;
	private BlockingQueue<BlockingQueue<Packet>> emptyQueue;
	private static boolean execute ;
	private boolean pickTCPdata;
	Map<String,List<Integer>> pendingContent ;

	public RMIServer(StateManager stateMgr,DataStore dStore,SegmentationReassembly sr,PolicyModule policy,BlockingQueue<BlockingQueue<Packet>> eQ,LinkDetector ld)
	{
		sar = sr;
		policyModule = policy;
		emptyQueue = eQ;
		stateManager = stateMgr ;
		execute = true ;
		ldetector = ld;
		pendingContent = new HashMap<String, List<Integer>>();
		pickTCPdata = true ;
	}

	@SuppressWarnings("deprecation")
	public void run()
	{
		while(execute)
		{	
			try
			{
				BlockingQueue<Packet> packetQueue = emptyQueue.take();
				if(pickTCPdata){
					pickTCPdata = false ;
					boolean flag = true ;
					List<String> requestedData = stateManager.getTCPDownloadRequests();
					//System.out.println("Size of requestedData: "+requestedData);
					if(requestedData.size() == 0){
						emptyQueue.put(packetQueue);
						continue ;
					}
					String request = requestedData.remove(0);
					//System.out.println(request);
					List<ContentState> stateObjList = stateManager.getStateObject(request);
					ContentState downState = stateManager.getStateObject(request,ContentState.Type.tcpDownload);
					if(downState != null){
						if(downState.currentSegments != downState.getTotalSegments()){
							flag = false ;
						}	
					}

					if(flag)
					{
						ContentState stateObject = stateObjList.remove(0);
						
						if(stateObject.currentSegments!=stateObject.getTotalSegments()){
							if(stateObject.currentSegments == 1)
								Thread.sleep(1000);
							String	bitMap = null ;
							Segmenter segmenter = sar.getSegmenter();
							String finalDestination = stateObject.getPreferredRoute().get(0).split(":")[0];
							policyModule.setPolicy(finalDestination, Connection.Type.values()[stateObject.getPreferredInterface()]);
							ControlHeader header = new ControlHeader(finalDestination,null,bitMap,stateObject.getOffset(),finalDestination,stateObject.getMetaDataFlag());
							segmenter.sendSegments(stateObject,stateObject.getContentId(),stateObject.getUploadId(),header,  packetQueue,stateObject.getTotalSegments(),stateObject.currentSegments);
							stateObjList.add(stateObjList.size(),stateObject);

						}
						else
						{	
							/*********************************Added in between*******************************************************/
							IUser stub = null ;
							Registry registry = null;
							List<Integer> pendingPackets = new ArrayList<Integer>();
							String contentName = stateObject.getUploadId();
							String finalDest = stateObject.getPreferredRoute().get(0);
								
							try {
								if(pendingContent.containsKey(contentName)){
									String bitMap = null ;
									Segmenter segmenter = sar.getSegmenter();
									pendingPackets = pendingContent.get(contentName);
									System.out.println("Number of pending packets is: "+pendingPackets.size());
									String finalDest1 = stateObject.getPreferredRoute().get(0).split(":")[0];
									policyModule.setPolicy(finalDest1, Connection.Type.values()[stateObject.getPreferredInterface()]);
									ControlHeader header1 = new ControlHeader(stateObject.getAppId(),null,bitMap,stateObject.getOffset(),finalDest1,stateObject.getMetaDataFlag());
									segmenter.sendPendingPackets(stateObject.getContentId(),stateObject.getUploadId(),header1,packetQueue,pendingContent);
									stateObjList.add(stateObjList.size(),stateObject);
								}	
								else{
									try{
										Thread.sleep(3000);//1000
										boolean bound = false;
										while(!bound)
										{
											try
											{
												Status st = Status.getStatus();
												//String usernode = st.updateUserLocation("select distinct(ipadd) from userlocation where userNode = '"+finalDest.split(":")[0]+"'", 2);
												String usernode = st.getUserDaemonLocation(finalDest.split(":")[0]);
												System.out.println("usernode ip is: "+usernode);
												registry = LocateRegistry.getRegistry(usernode);
												stub = (IUser) registry.lookup("userdaemon");
												bound = true;						
											}
											catch(Exception ex)
											{
												ex.printStackTrace();
												System.out.println("Exception in RMIServer.java::line168");
											}
										}
										System.out.println("User daemon found.");
										pendingPackets = stub.getUploadAcks(contentName,stateObject.getTotalSegments());
										System.out.println("Size of missing packets: "+pendingPackets.size());
									}
									catch(Exception e){
										e.printStackTrace();
										emptyQueue.put(packetQueue);
										System.out.println("Problem in RMI contact, but the Queue size is: "+emptyQueue.size());										
										stateObjList.add(stateObjList.size(),stateObject);
										
									}
									if(pendingPackets.size()!= 0){
										String bitMap = null ;
										Segmenter segmenter = sar.getSegmenter();
										pendingContent.put(contentName, pendingPackets);
										System.out.println("Number of pending packets first time is: "+pendingPackets.size());
										String finalDest1 = stateObject.getPreferredRoute().get(0).split(":")[0];
										policyModule.setPolicy(finalDest1, Connection.Type.values()[stateObject.getPreferredInterface()]);
										ControlHeader header1 = new ControlHeader(stateObject.getAppId(),null,bitMap,stateObject.getOffset(),finalDest1,stateObject.getMetaDataFlag());
										segmenter.sendPendingPackets(stateObject.getContentId(),stateObject.getUploadId(),header1,packetQueue,pendingContent);
										stateObjList.add(stateObjList.size(),stateObject);

									}
									else{
										System.out.println("There are no pending packets for the requesting content: "+contentName);
									}
								}
							} catch (Exception e) {
								e.printStackTrace();
							}
						}

						if(stateObjList.size()==0){
							emptyQueue.put(packetQueue);
							List<String> dtnrequest = stateManager.getDTNDownloadRequests();
							if(requestedData.size()==0 && dtnrequest.size()== 0){
								execute = false ;
								System.out.println("Request in RMI Server: "+request);
								stateManager.setTCPDownloadRequestList(request);
								System.out.println("In download finishing Queue: "+emptyQueue.size());
								suspend();
							}
						}
						else{
							requestedData.add(requestedData.size(),request);
						}	
					}
					else
					{
						emptyQueue.put(packetQueue);
						requestedData.add(requestedData.size(),request);
					}
				}
				else{
					pickTCPdata = true ;
					boolean flag = true ;
					List<String> dtnData = stateManager.getDTNDownloadRequests();
					if(dtnData.size() == 0){
						emptyQueue.put(packetQueue);
						continue ;
					}
					String request = dtnData.remove(0);
					
					List<ContentState> stateObjList = stateManager.getDtnStateObject(request);
					ContentState downState = stateManager.getStateObject(request,ContentState.Type.tcpDownload);
					if(downState != null){
						if(downState.currentSegments != downState.getTotalSegments()){
							flag = false ;
						}	
					}
					if(flag)
					{	
						ContentState stateObject = stateObjList.remove(0);
						String finalDest = stateObject.getPreferredRoute().get(0) ;
						if(stateObject.currentSegments!=stateObject.getTotalSegments()){
							String	bitMap = null ;
							Segmenter segmenter = sar.getSegmenter();
							finalDest = stateObject.getPreferredRoute().get(0) ;
							boolean addDest = ldetector.addDTNDestination(finalDest);
							if(addDest){
								String finalDestination = stateObject.getPreferredRoute().get(0).split(":")[0];
								policyModule.setPolicy(finalDestination, Connection.Type.values()[stateObject.getPreferredInterface()]);
								ControlHeader header = new ControlHeader(finalDestination,null,bitMap,stateObject.getOffset(),finalDestination,stateObject.getMetaDataFlag());
								segmenter.sendDTNSegments(stateObject,stateObject.getContentId(),stateObject.getUploadId(),header,  packetQueue,stateObject.getTotalSegments(),stateObject.currentSegments);
								stateObjList.add(stateObjList.size(),stateObject);
							}	
							else{
								emptyQueue.put(packetQueue);
								stateObjList.add(stateObjList.size(),stateObject);
								
							}
						}
						if(stateObjList.size()==0){
							emptyQueue.put(packetQueue);
							List<String> tcpRequest = stateManager.getTCPDownloadRequests();
							if(dtnData.size()==0 && tcpRequest.size()==0){
								execute = false ;
								stateManager.setDTNDownloadRequestList(request);
								ldetector.removeDTNdestination(finalDest);
								System.out.println("In download finishing Queue: "+emptyQueue.size());
								suspend();
							}
						}
						else{
							dtnData.add(dtnData.size(),request);
						}	
					}
					else
					{
						emptyQueue.put(packetQueue);
						dtnData.add(dtnData.size(),request);
					}
				}
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
	
	public boolean isNotRunning(){
		return execute;
	}
	
	public void setExecute()
	{
		execute = true ;
	}
}