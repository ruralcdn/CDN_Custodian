package NewStack;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import newNetwork.Connection;

public class ListeningScheduler{

	private BlockingQueue<Packet> packetQueue;
	private Map<String,Queue<PacketListener>> listenerConnectionPool;


	public ListeningScheduler(BlockingQueue<Packet> queue)
	{
		packetQueue = queue;
		listenerConnectionPool = new HashMap<String,Queue<PacketListener>>();

	}

	public BlockingQueue<Packet> getQueue()
	{
		return packetQueue;
	}

	public void close()
	{
		Set<String> listeners =  listenerConnectionPool.keySet();
		Iterator<String> it = listeners.iterator();
		while(it.hasNext())
		{
			String Id = it.next();
			Queue<PacketListener> packetListenerList = listenerConnectionPool.get(Id);
			Iterator<PacketListener> it1 = packetListenerList.iterator();
			while(it1.hasNext())
			{
				PacketListener listener = it1.next();
				it1.remove();
				listener.close();
			}
			it.remove();
		}
	}

	public void addConnection(String Id,Connection con)
	{
		Queue<PacketListener> connectionPool = listenerConnectionPool.get(Id);
		if(connectionPool == null)
			connectionPool = new LinkedList<PacketListener>();
		PacketListener listener = new PacketListener(packetQueue,con);
		listener.start();
		connectionPool.add(listener);
		listenerConnectionPool.put(Id,connectionPool);
	}

}