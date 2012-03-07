package NewStack;

import java.io.Serializable;
import java.util.List;


@SuppressWarnings("serial")
public class Packet implements Serializable{
		
	public enum PacketType{Data,Authentication};
	private PacketType type;
	private String name;
	private byte[] data;
	private int sequenceNumber;
	private byte[] packet;
	private List<String> route;    //route and source should have a 4 byte guid
	private String destination;
	private String connectionId;
	private int TCPPacketLen;
	private int AuthenticationPacketLen;
	private boolean DTNFlag;
	private boolean isMetaData;

	public Packet(PacketType t,String n,byte[]d,int seq_no,String dest,boolean isMeta)
	{
		type = t;
		name = n;
		data = d;
		destination = dest;
		sequenceNumber = seq_no;
		TCPPacketLen = 20+n.length()+d.length;
		packet = new byte[TCPPacketLen];
		DTNFlag = false;
		isMetaData = isMeta;
		createPacket();

	}
	//Authentication Packet
	public Packet(String conId)
	{
		type = PacketType.Authentication;
		connectionId = conId;
		AuthenticationPacketLen = 4+4+connectionId.length();
		packet = new byte[AuthenticationPacketLen];
		createAuthenticationPacket();
		DTNFlag = false;

	}
	//DTN PAcket
	public Packet(List<String> preferredRoute,String dest,PacketType t,String n,byte[]d,int seq_no,boolean isMeta)
	{
		type = t;
		name = n;
		data = d;
		sequenceNumber = seq_no;
		route = preferredRoute;
		destination = dest;
		TCPPacketLen = 20+name.length()+data.length;
		//DTNHeaderLen = 4+4+4*route.size();
		packet = new byte[TCPPacketLen]; //new byte[16+4+4+4*route.size()+n.length()+d.length];
		DTNFlag = true;
		isMetaData = isMeta;
		createPacket();		

	}

	public boolean isDTNPacket()
	{
		return DTNFlag;
	}
	
	public boolean isMetaData()
	{
		return isMetaData;
	}
	private void createAuthenticationPacket()
	{
		//type, connectionId's length and connectionId
		int len = 4;
		int off = 0;
		//type
		for (int i = 0; i < len; i++) {
			int offset = (len - 1 - i) * 8;
			packet[off] = (byte) ((type.ordinal() >>> offset) & 0xFF);
			off++;
		}
		//connectionid's length
		int length = connectionId.length();
		for (int i = 0; i < len; i++) {
			int offset = (len - 1 - i) * 8;
			packet[off] = (byte) ((length >>> offset) & 0xFF);
			off++;
		}
		//connectionId
		byte[] connectionIdBytes = connectionId.getBytes();
		for(int i = 0;i < connectionId.length();i++)
		{
			packet[off] = connectionIdBytes[i]; 
			off++;
		}
	}
	
	/*private void addDTNHeader()
	{
		/*order of sending - route's length,route,provenance,
		 * type,seqNo,name's length,data's length,name data
		 * 
		int len = 4;
		int off = 0;
		byte[] header = new byte[DTNHeaderLen];
		//route's length
		for (int i = 0; i < len; i++) {
			int offset = (len - 1 - i) * 8;
			header[off] = (byte) ((route.size() >>> offset) & 0xFF);
			off++;
		}
		//route
		for(int j = 0; j < route.size();j++)
		{
		for (int i = 0; i < len; i++) {
			int offset = (len - 1 - i) * 8;
			header[off] = (byte) ((route.get(j) >>> offset) & 0xFF);
			off++;
		}
		}
		//provenance
		for (int i = 0; i < len; i++) {
			int offset = (len - 1 - i) * 8;
			header[off] = (byte) ((provenance >>> offset) & 0xFF);
			off++;
		}
		
		byte[] tempPacket = new byte[DTNHeaderLen+TCPPacketLen];
		
		for(int i = 0;i < DTNHeaderLen;i++)
		{
			tempPacket[i] = header[i];
		}
		for(int i =0; i < TCPPacketLen;i++)
		{
			tempPacket[DTNHeaderLen+i] = packet[i];
		}
		packet = tempPacket;
	}*/
	public void removeDTNHeader()
	{
	/*	System.out.println("Before removal of header");
		System.out.println("AppId: "+Utils.byteArrayToInt(packet, DTNHeaderLen+0));
		System.out.println("Packet type: "+Utils.byteArrayToInt(packet, DTNHeaderLen+4));
		System.out.println("Seq. No: "+Utils.byteArrayToInt(packet, DTNHeaderLen+8));
		System.out.println("ContetnNameLen = "+Utils.byteArrayToInt(packet, DTNHeaderLen+12));
		System.out.println("DataLen "+Utils.byteArrayToInt(packet, DTNHeaderLen+16));

		byte[] tempPacket = new byte[TCPPacketLen];

		for(int i = 0;i < tempPacket.length;i++)
		{
			tempPacket[i] = packet[DTNHeaderLen+i];
		}
		packet = tempPacket;
		System.out.println("After removal of header");
		System.out.println("AppId: "+Utils.byteArrayToInt(packet, 0));
		System.out.println("Packet type: "+Utils.byteArrayToInt(packet, 4));
		System.out.println("Seq. No: "+Utils.byteArrayToInt(packet, 8));
		System.out.println("ContetnNameLen = "+Utils.byteArrayToInt(packet, 12));
		System.out.println("DataLen "+Utils.byteArrayToInt(packet, 16));*/

	}
	private void createPacket()
	{
		//order of sending data - type,ismetadata variable,seqNo,name's length,data's length,name,data
		int len = 4;
		int off = 0;
		//AppId
/*		for (int i = 0; i < len; i++) {
			int offset = (len - 1 - i) * 8;
			packet[off] = (byte) ((AppId >>> offset) & 0xFF);
			off++;
		}
		*/
		//type
		for (int i = 0; i < len; i++) {
			int offset = (len - 1 - i) * 8;
			packet[off] = (byte) ((type.ordinal() >>> offset) & 0xFF);
			off++;
		}
		//isMetaData
		int sendVariable = 0;
		if(isMetaData)
			sendVariable = 1;
		//System.out.println("Send Variable is "+sendVariable);
		for (int i = 0; i < len; i++) {
			int offset = (len - 1 - i) * 8;
			packet[off] = (byte) ((sendVariable >>> offset) & 0xFF);
			off++;
		}
		
		//seqNo
		for (int i = 0; i < len; i++) {
			int offset = (len - 1 - i) * 8;
			packet[off] = (byte) ((sequenceNumber >>> offset) & 0xFF);
			off++;
		}
		//name's length
		for (int i = 0; i < len; i++) {
			int offset = (len - 1 - i) * 8;
			packet[off] = (byte) ((name.length() >>> offset) & 0xFF);
			off++;
		}
		//data's length
		for (int i = 0; i < len; i++) {
			int offset = (len - 1 - i) * 8;
			packet[off] = (byte) ((data.length >>> offset) & 0xFF);
			off++;
		}
		//name
		byte[] namebytes = name.getBytes(); 
		for(int i = 0;i < name.length();i++)
		{
			packet[off] = namebytes[i]; 
			off++;
		}
		//data
		for(int i = 0;i < data.length;i++)
		{
			packet[off] = data[i];
			//System.out.println("packet[off] = "+ (char)packet[off]);
			off++;
		}
		//System.out.println("data = " + new String(data));
		//System.out.println("Packet = " + new String(packet));
		//	return packet;
	}

	public byte[] getBytePacket()
	{
		return packet;	
	}

	public PacketType getType()
	{
		return type;
	}
	public String getName()
	{
		return name;
	}
	public byte[] getData()
	{
		return data;
	}
	public int getSequenceNumber()
	{
		return sequenceNumber;
	}
	public List<String> getRoute()
	{
		return route;
	}
	public String getDestination()
	{
		return destination;
	}

}

