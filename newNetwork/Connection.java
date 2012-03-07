package newNetwork;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;

import NewStack.Packet;

public interface Connection{

	public enum Type{USB,DSL,Dialup};
	public void close() throws IOException;
	public Connection.Type getType();
	public Packet readPacket() throws IOException,InterruptedException, Exception;
	public void writePacket(Packet packet) throws IOException;
	public InputStream getInputStream() throws IOException;
	public OutputStream getOutputStream() throws IOException;
	public String getRemoteAddress();
	public int getRemotePort();
	public InetAddress getLocalAddress();
	public int getLocalPort();
}