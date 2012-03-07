package NewStack;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import NewStack.Packet.PacketType;
import StateManagement.ContentState;
import StateManagement.StateManager;
import StateManagement.Status;

import newNetwork.ControlHeader;

import prototype.datastore.DataStore;

public class Segmenter extends Thread{

	private DataStore store;
	private int segmentSize;
	private int dtnSize = 102400;
	BlockingQueue<BlockingQueue<Packet>> fullQueue;
	BlockingQueue<Packet> sendQueue ;
	Map<String, ContentState> mpUp ;
	public Segmenter(DataStore st,int size,BlockingQueue<BlockingQueue<Packet>> fQ, BlockingQueue<Packet> upDown)
	{
		store = st;
		segmentSize = size;
		fullQueue = fQ;
		sendQueue = upDown ;
	}

	public void sendSegments(String readName,String sendName,ControlHeader ctrlHeader,BlockingQueue<Packet> packetQueue,int totSeg,int curSeg)
	{
		BlockingQueue<Packet> segmentQueue = new ArrayBlockingQueue<Packet>(20);
		List<String> route = null;
		String destination = null;
		boolean sendMetaDataFlag = false;

		if(ctrlHeader != null)
		{
			route = ctrlHeader.getRoute();
			destination = ctrlHeader.getDestination();
			sendMetaDataFlag = ctrlHeader.getMetaDataFlag();
		}

		boolean sendMeta = false;
		sendMeta = true;


		if(sendMetaDataFlag && store.contains(readName+SegmentationReassembly.metadataSuffix) && sendMeta)
		{

			String fileName = readName+SegmentationReassembly.metadataSuffix;

			long fileSize = store.length(fileName);
			int offset = 0;
			int length = (int) fileSize;
			@SuppressWarnings("unused")
			int i = 0;
			while(length > 0 && segmentQueue.remainingCapacity() > 0)
			{
				byte[] segment;
				if(length < segmentSize)
					segment = store.read(fileName, offset,length);
				else
					segment = store.read(fileName, offset,segmentSize);	

				Packet packet = new Packet(route,destination,PacketType.Data,sendName,segment,offset,true);
				if(segmentQueue != null)
					segmentQueue.offer(packet);

				i++;
				offset += segmentSize;
				length -= segmentSize;
			}

		}
		
		mpUp = StateManager.getUpMap();
		ContentState stateObj = mpUp.get(sendName);
		int j = stateObj.currentSegments;
		for( ;j < (totSeg)&& segmentQueue.remainingCapacity() > 0;j++)
		{
			byte[] segment = store.read(readName, j*segmentSize, segmentSize);
			Packet packet = new Packet(route,destination,PacketType.Data,sendName,
					segment,j*segmentSize,false);
			if(segmentQueue != null)
					segmentQueue.offer(packet);
		}
		stateObj.currentSegments = j;	
		mpUp.put(sendName, stateObj);
		if(stateObj.currentSegments==stateObj.getTotalSegments())
		{
			System.out.println("ReadName and SendName is: "+readName+", "+sendName);
			System.out.println("Calling Segmenter send segments");
			Status st = Status.getStatus();
			//st.updateState("status", sendName, 1);
			st.updateState("status", readName, 1);
		}
		fullQueue.add(segmentQueue);

	}
	
	@SuppressWarnings("unused")
	public void sendSegments(ContentState stateObj,String readName,String sendName,ControlHeader ctrlHeader,BlockingQueue<Packet> packetQueue,int totSeg,int curSeg)
	{
		BlockingQueue<Packet> segmentQueue = new ArrayBlockingQueue<Packet>(20);
		List<String> route = null;
		String destination = null;
		boolean sendMetaDataFlag = false;
		if(ctrlHeader != null)
		{
			route = ctrlHeader.getRoute();
			destination = ctrlHeader.getDestination();
			sendMetaDataFlag = ctrlHeader.getMetaDataFlag();
		}

		boolean sendMeta = false;
		sendMeta = true;


		if(sendMetaDataFlag && store.contains(readName+SegmentationReassembly.metadataSuffix) && sendMeta)
		{

			String fileName = readName+SegmentationReassembly.metadataSuffix;

			long fileSize = store.length(fileName);
			int offset = 0;
			int length = (int) fileSize;
			int i = 0;
			while(length > 0 && segmentQueue.remainingCapacity() > 0)
			{
				byte[] segment;
				if(length < segmentSize)
					segment = store.read(fileName, offset,length);
				else
					segment = store.read(fileName, offset,segmentSize);	

				Packet packet = new Packet(route,destination,PacketType.Data,sendName,segment,offset,true);
				if(segmentQueue != null)
					segmentQueue.offer(packet);

				i++;
				offset += segmentSize;
				length -= segmentSize;
			}

		}
		int j = stateObj.currentSegments;
		for( ;j < (totSeg)&& segmentQueue.remainingCapacity() > 0;j++)
		{
			byte[] segment = store.read(readName, j*segmentSize, segmentSize);
			Packet packet = new Packet(route,destination,PacketType.Data,sendName,
					segment,j*segmentSize,false);
			if(segmentQueue != null)
					segmentQueue.offer(packet);
		}
		stateObj.currentSegments = j;	
		if(stateObj.currentSegments==stateObj.getTotalSegments())
		{
			System.out.println("Calling Segmenter send segments");
			Status st = Status.getStatus();
			//st.updateState("status", sendName, 1);
			st.updateState("status", readName, 1);
		}
		fullQueue.add(segmentQueue);
		//System.out.println("Hi am here: "+fullQueue.size());
	}
	
	public void sendPackets(String readName, String sendName, ControlHeader ctrlHeader,int totalSegments,BlockingQueue<Packet> segmentQueue, int downSeg, int upSeg ){
		List<String> route = null;
		BitSet bitSet = new BitSet();
		String destination = null;
		Packet packet = null ;
		if(ctrlHeader != null)
		{
			route = ctrlHeader.getRoute();
			destination = ctrlHeader.getDestination();
			bitSet = ctrlHeader.getBitSet();
			
		}
		mpUp = StateManager.getUpMap();
		ContentState contentState = mpUp.get(sendName);
		BitSet upMap = contentState.bitMap;
		for(int j = 0 ;j < totalSegments && segmentQueue.remainingCapacity() > 0;j++)
		{
			if(!bitSet.get(j)){
				byte[] segment = store.read(readName, j*segmentSize, segmentSize);
				packet = new Packet(route,destination,PacketType.Data,sendName,
						segment,j*segmentSize,false);
				if(segmentQueue != null)
					segmentQueue.offer(packet);
				contentState.currentSegments++;
				upMap.set(j);
			}
		}
		contentState.bitMap = upMap;
		mpUp.put(sendName, contentState);
		if(contentState.currentSegments == totalSegments){
			System.out.println("Calling Segmenter send packets");
			Status st = Status.getStatus();
			st.updateState("status", sendName, 1);
		}	
		
		fullQueue.add(segmentQueue);
	}
	
	public void sendPendingPackets(String readName,String sendName,ControlHeader ctrlHeader,BlockingQueue<Packet> packetQueue,Map<String,List<Integer>> pendingContent){
		BlockingQueue<Packet> segmentQueue = packetQueue;
		List<String> route = null;
		String destination = null;
		if(ctrlHeader != null)
		{
			route = ctrlHeader.getRoute();
			destination = ctrlHeader.getDestination();
		}

		List<Integer> pendingList = new ArrayList<Integer>();
		pendingList = pendingContent.get(sendName) ;
		int index = pendingList.size();
		for(int j = 0 ; j < index && segmentQueue.remainingCapacity() > 0;j++)
		{
			int segNo =pendingList.remove(0);
			//System.out.println("Sending lost packet segment num: "+segNo);
			byte[] segment = store.read(readName, segNo*segmentSize, segmentSize);
			Packet packet = new Packet(route,destination,PacketType.Data,sendName,
					segment,segNo*segmentSize,false);
			if(segmentQueue != null)
					segmentQueue.offer(packet);
			
		}
		if(pendingList.size()!=0)
			pendingContent.put(sendName,pendingList);
		else
			pendingContent.remove(sendName);
		fullQueue.add(segmentQueue);
	}
	
	public void sendDTNSegments(ContentState stateObj,String readName,String sendName,ControlHeader ctrlHeader,BlockingQueue<Packet> packetQueue,int totSeg, int curSeg)
	{
		
		BlockingQueue<Packet> segmentQueue = packetQueue;
		List<String> route = null;
		String destination = null;
		if(ctrlHeader != null)
		{
			route = ctrlHeader.getRoute();
			destination = ctrlHeader.getDestination();
		}

		int j = stateObj.currentSegments;
		for( ;j < (totSeg)&& segmentQueue.remainingCapacity() > 0;j++)
		{
			byte[] segment = store.read(readName, j*dtnSize, dtnSize);
			Packet packet = new Packet(route,destination,PacketType.Data,sendName,
					segment,j*dtnSize,false);
			if(segmentQueue != null)
					segmentQueue.offer(packet);
		}
		stateObj.currentSegments = j;		
		if(stateObj.currentSegments==stateObj.getTotalSegments())
		{
			System.out.println("Data Transfer into DTN Channel");
			Status st = Status.getStatus();
			st.updateState("status",sendName,-1);
		}
		fullQueue.add(segmentQueue);
	}
	
}