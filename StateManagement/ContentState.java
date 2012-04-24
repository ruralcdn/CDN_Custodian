package StateManagement;

import java.util.BitSet;
import java.util.List;

public class ContentState{
	
	public enum Type{tcpUpload,tcpDownload,dtn}
	
	private String contentId;
	private int offset;
	private int preferredInterface;
	private List<String> preferredRoute;
	private int totalSegments; 
	public int currentSegments ;
	private Type stateType;
	private String AppId;
	private String uploadId;
	private boolean sendMetaDataFlag;
	public BitSet bitMap ;

	
	public ContentState(String content,int off,BitSet bits,int conInterface,List<String> route,int size,int curSeg,Type type,String appId,boolean sendMetaData)
	{
		contentId = content;
		bitMap = bits;
		offset = off;
		preferredInterface = conInterface;
		preferredRoute = route;
		totalSegments = size;
		currentSegments = curSeg ;
		stateType = type;
		AppId = appId;
		sendMetaDataFlag = sendMetaData;
	}
	
	//for USB modification
	
	public ContentState(String content,int off,int conInterface,List<String> route,int curSeg,Type type,String appId,boolean sendMetaData){
		contentId = content;
		//bitMap = bits;
		offset = off;
		preferredInterface = conInterface;
		preferredRoute = route;
		//totalSegments = size;
		currentSegments = curSeg ;
		stateType = type;
		AppId = appId;
		sendMetaDataFlag = sendMetaData;
	}
	
	public ContentState(String contentName,String uploadName,int off,BitSet bits,int conInterface,List<String> route,int size,int curSeg,Type type,String appId,boolean sendMetaData)
	{
		if((type == Type.tcpUpload) || (type == Type.dtn))
		{
			contentId = contentName;
			uploadId = uploadName;
			bitMap = bits;
			offset = off;
			preferredInterface = conInterface;
			preferredRoute = route;
			totalSegments = size;
			currentSegments = curSeg ;
			stateType = type;
			AppId = appId;
			sendMetaDataFlag = sendMetaData;

		}
	}
	//FOR USB MODIFICATION
	public ContentState(String contentName,String uploadName,int off,int conInterface,List<String> route,int curSeg,Type type,String appId,boolean sendMetaData)
	{
		if((type == Type.tcpUpload) || (type == Type.dtn))
		{
			contentId = contentName;
			uploadId = uploadName;
			//bitMap = bits;
			offset = off;
			preferredInterface = conInterface;
			preferredRoute = route;
			//totalSegments = size;
			currentSegments = curSeg ;
			stateType = type;
			AppId = appId;
			sendMetaDataFlag = sendMetaData;

		}
	}
	
	public boolean getMetaDataFlag()
	{
		return sendMetaDataFlag;
	}
	public String getUploadId()
	{
		return uploadId;
	}
	
	public String getAppId()
	{
		return AppId;
	}
	
	public String getContentId()
	{
		return contentId;
	}
	public BitSet getBitMap()
	{
		return bitMap;
	}
	
	public int getCurrentSegments(){
		return currentSegments ;
	}
	public int getPreferredInterface()
	{
		return preferredInterface;
	}
	public List<String> getPreferredRoute()
	{
		return preferredRoute;
	}
	public int getTotalSegments()
	{
		return totalSegments;
	}
	public Type getStateType()
	{
		return stateType;
	}
	public int getOffset()
	{
		return offset;
	}
	
}
