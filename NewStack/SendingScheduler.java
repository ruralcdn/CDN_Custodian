package NewStack;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import StateManagement.ContentState;
import StateManagement.StateManager;
import newNetwork.Connection;

public class SendingScheduler extends Thread{

	private BlockingQueue<BlockingQueue<Packet>> fullQueues;
	private BlockingQueue<BlockingQueue<Packet>> emptyQueues;
	private Map<String,Queue<Connection>> senderConnectionPool;
	boolean execute;
	private PolicyModule policyModule;
	StateManager stateManager;
	int segmentSize;
	Map<String, ContentState> mpUp;

	public SendingScheduler(BlockingQueue<BlockingQueue<Packet>> fQueues,BlockingQueue<BlockingQueue<Packet>> eQueues,
			PolicyModule policy,StateManager manager,int segmentsize)
	{
		fullQueues = fQueues;
		emptyQueues = eQueues;
		senderConnectionPool = new HashMap<String,Queue<Connection>>();
		policyModule = policy;
		stateManager = manager;
		segmentSize = segmentsize;
		execute = true;
		mpUp =new HashMap<String, ContentState>();
	}

	public void addConnection(String conId,Connection con)
	{
		Queue<Connection> connections = senderConnectionPool.get(conId);
		if(connections == null)
		{
			connections = new LinkedList<Connection>();	
		}
		connections.add(con);
		senderConnectionPool.put(conId,connections);
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
			Packet packet = null;
			BlockingQueue<Packet> queue;
			try {
				queue = fullQueues.take();

				while(!queue.isEmpty())
				{
					packet = queue.poll();
					sendPakcet(packet);
				}
				emptyQueues.put(queue);
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}	
	}

	private void sendPakcet(Packet packet)
	{
		if(packet != null)
		{
			String destination = packet.getDestination();
			Connection.Type type = policyModule.getConnectionType(packet.getDestination());
			Queue<Connection> connections = senderConnectionPool.get(destination);
			if(connections != null)
			{
				int size = connections.size();
				boolean done = false;
				int count = 0;
				Connection con;
				while(!done && count < size)
				{
					count++;
					con = connections.poll();
					if(con.getType() != type)
					{
						connections.offer(con);
						continue;
					}

					if(!(type == Connection.Type.USB))
					{
						packet.removeDTNHeader();
						try
						{
							con.writePacket(packet);
						}catch(IOException e)
						{
							e.printStackTrace();
							connections.remove(con);
							Scheduler.removeConnection(destination, con);
							if(connections.size()==0)
							{	
								List<String> destinationConnectionIds =LinkDetector.getDestinationIds();
								String st = "" ;
								for(int i = 0 ; i < destinationConnectionIds.size() ; i++)
								{
									if(destinationConnectionIds.get(i).startsWith(destination))
										st = destinationConnectionIds.get(i);
								}
								
								destinationConnectionIds.remove(st);
								System.out.println("destinationConnectionIds in Sending Scheduler: " + destinationConnectionIds);
								LinkDetector.setDestinationIds(destinationConnectionIds);
								senderConnectionPool.remove(destination);
							}	
							continue;
						}
						done = true;
						connections.offer(con);
					}
					else
					{
						try
						{
							con.writePacket(packet);
						}
						catch(IOException e)
						{
							connections.offer(con);
							continue;
					
						}
						done = true;
						connections.offer(con);
					}
				}
			}
		}
	}

}