package NewStack;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import NewStack.Packet;
import StateManagement.ContentState;
import StateManagement.StateManager;
import prototype.datastore.DataStore;
import StateManagement.Status;

public class Reassembler extends Thread{

	private BlockingQueue<Packet> packetQueue;
	//private BlockingQueue<Packet> recQueue;
	private DataStore store;
	private boolean execute;
	private int segmentSize;		
	private BlockingQueue<String> fileDownloads;
	StateManager stateManager;
	Map<String, ContentState> mpContent;	
	Status stat;
	
	public Reassembler(BlockingQueue<Packet> queue,DataStore st,StateManager manager,int segmentsize,BlockingQueue<String> downloads, BlockingQueue<Packet> upDown)
	{
		packetQueue = queue;
		store = st;
		execute = true;
		stateManager = manager;
		segmentSize = segmentsize;
		fileDownloads = downloads;
		mpContent = new HashMap<String, ContentState>();
		stat = Status.getStatus();
		/*try {
			System.setOut(new PrintStream(new FileOutputStream("system_out.txt")));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}*/
		//recQueue = upDown ;
		
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
				Packet packet = packetQueue.take();				
				mpContent = StateManager.getDownMap();
				if(mpContent == null)
					continue;
				if(packet.isMetaData())
				{
					System.out.println("Received a metaData Packet");
					String data = packet.getName();
					long offset = packet.getSequenceNumber();
					byte[] segment = packet.getData();
					store.write(data+SegmentationReassembly.metadataSuffix,offset,segment);
				}
				else
				{
					String data = packet.getName();
					ContentState conProp = mpContent.get(data);
					BitSet bs = conProp.bitMap;
					
					
					long offset = packet.getSequenceNumber();
					byte[] segment = packet.getData();
					store.write(data, offset, segment);
					if(stateManager != null)
					{
						try	
						{
							int currentsegments = conProp.currentSegments;
							if(currentsegments == conProp.getTotalSegments()){
								
							}								
							else
							{
								if(bs.get((int) (offset/segmentSize))==false)
								{
									currentsegments++;
									bs.set((int) (offset/segmentSize));
								}
								conProp.currentSegments = currentsegments;
								conProp.bitMap = bs ;
								mpContent.put(data,conProp);
								if(currentsegments == conProp.getTotalSegments())
								{
									System.out.println("In Reassembler.java Received the Complete File! :D :D :D :D :D :D :D :D :D :D :D :D ");
									Status st = Status.getStatus();
									String fileType = st.getContentType("uploadrequest",data);
									if(fileDownloads != null)
									{	if(fileType.length()!=0)
											fileDownloads.put(data+"."+fileType);
										else
											fileDownloads.put(data);
									}	
									if(fileType.length()!=0)
										st.insertData("localdata", data+"."+fileType);
									else
										st.insertData("localdata", data);
									
									st.updateState("status",data, 0); 
									//mpContent.remove(data);
											 
								}

							}
						}catch(Exception e){ 
							e.printStackTrace();
						}
					}
				}
		}
		catch (Exception e) {
			e.printStackTrace();	
			}
		}
	}
}