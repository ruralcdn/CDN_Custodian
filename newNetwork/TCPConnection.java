package newNetwork;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;

import prototype.utils.Utils;

import NewStack.Packet;

public class TCPConnection implements Connection{

	private Socket socket;
	private int socketId;
	private Connection.Type type;

	public TCPConnection(int Id,InetAddress address, int port, 
			InetAddress localAddr, int localPort) throws IOException
			{	
		socketId = Id;
		socket = new Socket(address,port,localAddr,localPort);
		socket.setReuseAddress(true);
		type = Connection.Type.DSL;
			}
	public TCPConnection(InetAddress ip,int port) throws IOException
	{
		socket = new Socket(ip,port);
		type = Connection.Type.DSL;
	}
	public TCPConnection(Socket s)
	{
		socket = s;
		type = Connection.Type.DSL;
	}
	public int getConnectionId()
	{
		return socketId;
	}
	public int getRemoteDTNId()
	{
		return -1;
	}
	public Connection.Type getType()
	{
		return type;
	}
	public Packet readPacket() throws Exception
	{
		//reads Data Packet with no DTN Header expected
		//read type,sequence number,name's length,data's length,content name,data
		InputStream inStream = getInputStream();
		Packet packet;
		int length = 4;
		byte[] buffer = new byte[length];
		int read = 0;
		int numRead = 0;
		int len = buffer.length;
		int off = 0;
		//read AppId
		/*	read = 0;
		numRead = 0;
		len = buffer.length;
		off = 0;
		while((numRead = inStream.read(buffer, off, len)) >= 0 && read < buffer.length)
		{
		len -= numRead;	
		off += numRead;
		read += numRead;
		}
		int AppId = Utils.byteArrayToInt(buffer, 0);
*/	//	System.out.println("AppId: "+AppId);
		//read type
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
		
		int ordinal = Utils.byteArrayToInt(buffer, 0);
		
		//read isMetaData Flag
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
		int recvVariable = Utils.byteArrayToInt(buffer, 0);
		boolean isMetaData;
		if(recvVariable == 0)
			isMetaData = false;
		else
			isMetaData = true;

		
		
		//System.out.println("Packet type: "+ordinal);
		//Packet.PacketType type = Packet.PacketType.values()[ordinal];
		//read sequence number
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
		int sequenceNumber = Utils.byteArrayToInt(buffer, 0);
		//System.out.println("Seq. No: "+sequenceNumber);
		//read name's length
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
		int ContentNameLength = Utils.byteArrayToInt(buffer, 0);
	//	System.out.println("ContetnNameLen = "+ContentNameLength);
		//read data's length
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
		int DataLength = Utils.byteArrayToInt(buffer, 0);
	//	System.out.println("DataLen = "+DataLength);
		//read content name
		buffer = new byte[ContentNameLength];
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
		String contentName = new String(buffer);
		//read data
		buffer = new byte[DataLength];
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
		byte[] data = buffer;
		
		//System.out.println("Read packet :) from socket stream");
		Packet.PacketType type = Packet.PacketType.values()[ordinal];
		packet = new Packet(type,contentName,data,sequenceNumber,null,isMetaData);
		return packet;
		
	}
	public void writePacket(Packet packet) throws IOException
	{
	/*	System.out.println("AppId: "+packet.getAppId());
		System.out.println("Packet type: "+packet.getType().ordinal());
		System.out.println("Seq. No: "+packet.getSequenceNumber());
		if(packet.getName() != null)
		System.out.println("ContetnNameLen = "+packet.getName().length());
		if(packet.getData() != null)
		System.out.println("DataLen "+packet.getData().length);
		if(packet.getType() == PacketType.Data)
		{
		byte[] packetarray = packet.getBytePacket();
		System.out.println("calculated AppId: "+Utils.byteArrayToInt(packetarray, 0));
		System.out.println("calculated Packet type: "+Utils.byteArrayToInt(packetarray, 4));
		System.out.println("calculated Seq. No: "+Utils.byteArrayToInt(packetarray, 8));
		System.out.println("calculated ContetnNameLen = "+Utils.byteArrayToInt(packetarray, 12));
		System.out.println("calculated DataLen "+Utils.byteArrayToInt(packetarray, 16));
		}*/
		OutputStream out = getOutputStream();
		out.write(packet.getBytePacket());
		out.flush();
	}

	public OutputStream getOutputStream() throws IOException
	{
		return socket.getOutputStream();
	}
	public InputStream getInputStream() throws IOException
	{
		return socket.getInputStream();
	}
	public void close() throws IOException
	{
		socket.close();
	}
	public String getRemoteAddress()
	{
		return socket.getInetAddress().toString();
	}
	public int getRemotePort()
	{
		return socket.getPort();
	}
	public InetAddress getLocalAddress()
	{
		return socket.getLocalAddress();
	}
	public int getLocalPort()
	{
		return socket.getLocalPort();
	}
}