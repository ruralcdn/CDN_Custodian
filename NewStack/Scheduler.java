package NewStack;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import StateManagement.StateManager;

import AbstractAppConfig.AppConfig;
import newNetwork.Connection;

public class Scheduler{

	private SendingScheduler sscheduler;
	private ListeningScheduler lscheduler;
	static Map<String,List<Connection>> connectionPool;
	private BlockingQueue<Packet> dataInQueue;   					 //queue for receiving data packets
	private BlockingQueue<BlockingQueue<Packet>> fullQueues;	  			 //queues for sending data packets
	private BlockingQueue<BlockingQueue<Packet>> emptyQueues;
	boolean execute;

	public Scheduler(PolicyModule policy,StateManager manager,int segmentSize) 
	{
		connectionPool = new HashMap<String,List<Connection>>();					//configure driven
		int outQueueSize = Integer.parseInt(AppConfig.getProperty("NetworkStack.SendingScheduler.QueueSize"));
		int inQueueSize = Integer.parseInt(AppConfig.getProperty("NetworkStack.ListeningScheduler.QueueSize"));
		dataInQueue = new ArrayBlockingQueue<Packet>(inQueueSize);
		int maxUploads = 5;  //configure driven
		emptyQueues = new ArrayBlockingQueue<BlockingQueue<Packet>>(maxUploads);
		for(int i = 0;i < maxUploads;i++)
		{
			BlockingQueue<Packet> queue = new ArrayBlockingQueue<Packet>(outQueueSize);
			emptyQueues.add(queue);
		}
		fullQueues = new ArrayBlockingQueue<BlockingQueue<Packet>>(maxUploads);
		sscheduler = new SendingScheduler(fullQueues,emptyQueues,policy,manager,segmentSize);
		lscheduler = new ListeningScheduler(dataInQueue);
		execute = false;
	}

	Map<String,List<Connection>> getConnectionPool()
	{
		return connectionPool;
	}
	
	public void close() 
	{

		Set<String> conIds = connectionPool.keySet();
		Iterator<String> it = conIds.iterator();
		while(it.hasNext())
		{
			String conId = it.next();
			it.remove();
			List<Connection> cons = connectionPool.get(conId);
			Iterator<Connection> it1 = cons.iterator();
			while(it1.hasNext())
			{
				try {
					it1.next().close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				it1.remove();
			}

		}
		sscheduler.close();
		lscheduler.close();

	}

	public BlockingQueue<Packet> getDataInQueue()
	{
		return dataInQueue;
	}
	
	public BlockingQueue<BlockingQueue<Packet>> getDataFullQueues()
	{
		return fullQueues;
	}
	
	public BlockingQueue<BlockingQueue<Packet>> getDataEmptyQueues()
	{
		return emptyQueues;
	}

	public void addConnection(String Id,Connection con)
	{
		System.out.println("Adding the connection in the schedular to the conId Id in Schedular.java");
		List<Connection> cons = connectionPool.get(Id);
		if(cons == null)
			cons = new ArrayList<Connection>();
		cons.add(con);
		connectionPool.put(Id,cons);
		sscheduler.addConnection(Id,con);
		lscheduler.addConnection(Id,con);
		if(connectionPool.values().size() == 1 && !execute)
		{
			execute = true;
			System.out.println("starting the schedulars for sending and receiving in Scheduler.java");
			sscheduler.start();
		}
	}
	
	public static void removeConnection(String destination, Connection con)
	{
		List<Connection> cons = connectionPool.get(destination);
		for(int i = 0 ; i < cons.size() ; i++)
		{
			if(cons.get(i).equals(con)){
				cons.remove(i);
				System.out.println("Removing Connection");
			}	
		}
		System.out.println("Connection' size: "+cons.size());
		if(cons.size() == 0){
			connectionPool.remove(destination);
			Set<String> destinationSet = connectionPool.keySet();
			Iterator<String> it = destinationSet.iterator();
			while(it.hasNext())
				System.out.println("destinationset in Scheduler: "+it.next());
		}
			
	}
	
	public void removeDTNConnection(String destination){
		List<Connection> cons = connectionPool.get(destination);
		for(int i = 0 ; i < cons.size() ; i++){
			if(cons.get(i).getType() == Connection.Type.USB)
				cons.remove(i);
		}
	}

}