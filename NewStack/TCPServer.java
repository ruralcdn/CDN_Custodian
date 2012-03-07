package NewStack;

import java.io.IOException;
import java.io.InputStream;

import newNetwork.Connection;
import newNetwork.TCPServerConnection;
import prototype.utils.Utils;

public class TCPServer implements Runnable{
	
	TCPServerConnection server;
	Scheduler scheduler;
	boolean execute;
	private int serverport;
	
	public TCPServer(Scheduler sched,int port){
		serverport = port;
		 server = new TCPServerConnection(serverport);
		System.out.println("Server Ready");
		scheduler = sched;
		execute = true;
	}
	
	public int getServerPort()
	{
		return serverport;
	}
	public void run()
	{
	
		try{
			while(execute)
			{
				Connection con = server.accept();
				System.out.println("connection accepted");
				//connectionPool.add(con);
				try {
					InputStream inStream = con.getInputStream();
					int length = 4;
					byte[] buffer = new byte[length];
					//read type
					int read = 0;
					int numRead = 0;
					int len = buffer.length;
					int off = 0;
					while((numRead = inStream.read(buffer, off, len)) >= 0 && read < buffer.length)
					{
					len -= numRead;	
					off += numRead;
					read += numRead;
					}
					Packet.PacketType type = Packet.PacketType.values()[Utils.byteArrayToInt(buffer, 0)];
					if(type.equals(Packet.PacketType.Authentication))
					{
						//read connectionId's length
						read = 0;
						numRead = 0;
						len = buffer.length;
						off = 0;
						while((numRead = inStream.read(buffer, off, len)) >= 0 && read < buffer.length)
						{
						len -= numRead;	
						off += numRead;
						read += numRead;
						}
						int connectionIdlength = Utils.byteArrayToInt(buffer, 0);
						
						//read ConnectionId
						buffer = new byte[connectionIdlength];
						read = 0;
						numRead = 0;
						len = buffer.length;
						off = 0;
						while((numRead = inStream.read(buffer, off, len)) >= 0 && read < buffer.length)
						{
						len -= numRead;	
						off += numRead;
						read += numRead;
						}
						String sourceId = new String(buffer);
						
						//System.out.println("Read Authentication packet :) from socket stream");

						scheduler.addConnection(sourceId, con);
						System.out.println("User Authentication Done userId: "+sourceId);
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		}catch(IOException e)
		{
				System.out.println("IOException while accepting connection");
				e.printStackTrace();	
		}finally{
			try {
				server.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("IOException while closing server");
				e.printStackTrace();
			}
		}	
	}

	public void close() {
		// TODO Auto-generated method stub
		execute = false;
		
	}
}