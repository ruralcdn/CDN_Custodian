package NewStack;

import java.util.concurrent.BlockingQueue;

import newNetwork.Connection;

public class PacketListener extends Thread{
	
	private BlockingQueue<Packet> packetQueue;
	boolean execute = false;
	private Connection connection;
	
	public PacketListener(BlockingQueue<Packet> queue,Connection con)
	{
		packetQueue = queue;
		connection = con;
		execute = true;
	}
	
	public void close()
	{
		execute = false;
	}
	
	public void run()
	{
		Packet packet;
		System.out.println("listening in packetListener.java");
		while(execute)
		{				
			try
			{
				packet = connection.readPacket();   
				if(packet != null)
					packetQueue.put(packet);
					
				
			}
			catch(Exception e)
			{
				System.out.println("No connection is available");
				execute = false ;
			}

		}	
	}
	
	
}