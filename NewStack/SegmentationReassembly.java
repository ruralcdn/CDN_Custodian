package NewStack;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import StateManagement.StateManager;
import prototype.datastore.DataStore;

public class SegmentationReassembly{

	private Scheduler scheduler;
	private DataStore store;
	private int segmentSize;
	private int dtnSegmentSize = 102400 ;
	Reassembler reassembler;
	Segmenter segmenter;
	private BlockingQueue<Packet> upDown = new ArrayBlockingQueue<Packet>(1000);
	public static final String metadataSuffix = new String(".metadata");
	
	public SegmentationReassembly(StateManager manager,DataStore st,Scheduler sched,int segmentsize,BlockingQueue<String> downloads)
	{
		store = st;
		scheduler = sched;
		segmentSize = segmentsize;
		reassembler = new Reassembler(scheduler.getDataInQueue(),store,manager,segmentSize,downloads, upDown);
		reassembler.start();
		segmenter = new Segmenter(store,segmentSize,scheduler.getDataFullQueues(),upDown);
	}

	public long countSegments(String dataname)
	{
		long smallchunk = (store.length(dataname)%segmentSize);
		if(smallchunk == 0)
			return (store.length(dataname)/segmentSize);
		else
			return (((store.length(dataname) - smallchunk)/segmentSize) + 1);
	}

	public long countdtnSegments(String dataname)
	{
		long smallchunk = (store.length(dataname)%dtnSegmentSize);
		if(smallchunk == 0)
			return (store.length(dataname)/dtnSegmentSize);
		else
			return (((store.length(dataname) - smallchunk)/dtnSegmentSize) + 1);
	}
	public void close()
	{
		reassembler.close();
	}
	public Segmenter getSegmenter()
	{
		return segmenter;
	}

}