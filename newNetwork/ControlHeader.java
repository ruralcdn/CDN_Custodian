package newNetwork;

import java.util.BitSet;
import java.util.List;


public class ControlHeader{
	
	private List<String> route;
	private String bits;
	private BitSet bitSet ;
	private int offset;
	private String destination;
	private String AppId;
	private boolean sendMetaData;
	
	public ControlHeader(String Id,List<String> preferredRoute,
			String bitMap,int off,String dest,boolean metadata)
	{
		AppId = Id;
		route = preferredRoute;
		bits = bitMap;
		offset = off;
		destination = dest;
		sendMetaData = metadata;
	}
	//Newly Added
	public ControlHeader(String Id,List<String> preferredRoute,
			BitSet bitMap,int off,String dest,boolean metadata)
	{
		AppId = Id;
		route = preferredRoute;
		bitSet = bitMap;
		offset = off;
		destination = dest;
		sendMetaData = metadata;
	}
	

	public boolean getMetaDataFlag()
	{
		return sendMetaData;
	}
	public String getAppId()
	{
		return AppId;
	}
	public String getDestination()
	{
		return destination;
	}
	
	public List<String> getRoute()
	{
		return route;
	}
	
	public String getBitMap()
	{
		return bits;
	}
	
	public int getBitMapOffset()
	{
		return offset;
	}
	
	public BitSet getBitSet(){
		return bitSet;
	}
}