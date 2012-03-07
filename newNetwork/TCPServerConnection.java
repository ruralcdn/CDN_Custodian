package newNetwork;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class TCPServerConnection{
	
	private ServerSocket socket;
	
	public TCPServerConnection(int port)
	{
		try
		{
		socket = new ServerSocket(port);    
		}catch(IOException e)
		{
			System.out.println("IOException: "+ e.getMessage());
		}
	}
	
	public Connection accept() throws IOException
	{
		Socket s = socket.accept();
		return new TCPConnection(s);

	}
	
	public void close() throws IOException
	{
		socket.close();
	}
}