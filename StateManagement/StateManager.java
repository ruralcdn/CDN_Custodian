package StateManagement;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import NewStack.DataUploader;
import NewStack.NewStack;
import NewStack.RMIServer;
import prototype.utils.Utils;

public class StateManager{

	private File status;
	private static final String tcpUploadRequest = new String("TCPUploadRequest");
	private static final String tcpDownloadRequest = new String("TCPDownloadRequest");
	private static final String dtnData = new String("DTNData");
	private static final String uploadNameSuffix = new String(".UploadName");
	private String table;
	String[] att = new String[8];
	static Map<String,ContentState> contUpStateMap;
	static Map<String,ContentState> contDownStateMap;
	private Map<String,String> requestMap ;
	private List<String> uploadRequest ; 
	private List<String> downloadRequest ;
	private List<String> dtndownRequest ;
	static DataUploader dataUp ;
	static RMIServer rmiServer ;
	static Map<String, List<ContentState>> contUpMultiMap ; 
	static Map<String, List<ContentState>> dtnUpMultiMap ; 
	public StateManager(String tableName)
	{
		table = tableName;
		contUpMultiMap = new HashMap<String,List<ContentState>>();
		dtnUpMultiMap = new HashMap<String,List<ContentState>>();
		contUpStateMap = new HashMap<String,ContentState>();
		Status st = Status.getStatus();
		contDownStateMap = st.getPendingStatus("select * from status where type = '0'");
		requestMap = new HashMap<String,String>();
		uploadRequest = new ArrayList<String>();
		downloadRequest = new ArrayList<String>();
		dtndownRequest = new ArrayList<String>();
		requestMap = st.getUploadRequsets("uploadrequest");
		Set<String> requestKey = requestMap.keySet(); 
		Iterator<String> it = requestKey.iterator();
		while(it.hasNext()){
			uploadRequest.add(it.next());
		}
		downloadRequest = st.getDownloadRequests("downloadrequest");
		contUpMultiMap = st.getDownMap(downloadRequest);
		dtndownRequest = st.getDownloadRequests("dtndownrequest");
	}

	public List<String> getTCPUploadRequests() 
	{
		return uploadRequest;
	}

	public List<String> getTCPDownloadRequests(){
		return downloadRequest;
	}
	
	public List<String> getDTNDownloadRequests(){
		return dtndownRequest;
	}
	@SuppressWarnings("unused")
	public  List<String> getDTNData()
	{
		String dtnDataString = "[]" ;
		List<String> dtnDataList;
		if(dtnDataString != null)
			dtnDataList = Utils.parse(dtnDataString);
		else
			dtnDataList = new ArrayList<String>();

		return dtnDataList;
	}

	public  synchronized ContentState getStateObject(String contentId,ContentState.Type stateType)///sYNC
	{
		Status st = Status.getStatus();
		String uploadId ;
		ContentState stateObj;
		BitSet bitMap = null ;	
		int offset = -1 ;
		int preferredInterface = -1;
		int totalSegments = -1;
		int currentSegments = 0 ;
		String appId = null;
		List<String> preferredRoute = null;
		String str;
		int a = 0 ;
		int type ;
		if(stateType == ContentState.Type.tcpUpload)
			type = 1;  
		else
			type = 0 ;
		boolean metaDataFlag = false;
				
		if(stateType != ContentState.Type.tcpDownload &&  requestMap.get(contentId)!= null)
			uploadId = requestMap.get(contentId);
		else
			uploadId = contentId ;
		try{
			if(!contUpStateMap.containsKey(uploadId) && !contDownStateMap.containsKey(uploadId) )
			{
				att = st.execQuery("select * from "+ table+ " where contentid = '"+ uploadId + "' and type = '" + type + "'"); // In case of CUstodian , Service Instatnce, DataServer add this line				
				if(att!=null && att[0].length() != 0){
					 offset = Integer.parseInt(att[1]);
					 preferredInterface = Integer.parseInt(att[2]);
					 totalSegments = Integer.parseInt(att[3]);
					 currentSegments = Integer.parseInt(att[0]);
					 appId = att[4];
					 a = Integer.parseInt(att[5]);
					 str = att[6]+":"+att[7];
					 preferredRoute = new ArrayList<String>();
					 preferredRoute.add(str);
					 bitMap = new BitSet(totalSegments);
					 bitMap.clear();
					 bitMap.set(0,currentSegments,true);					 
				}				
			}			
		}catch(NumberFormatException e){
		}
		
		if(a==1)
			metaDataFlag = true;		

		if(stateType != ContentState.Type.tcpDownload)
		{
			String uploadName = uploadId;
			if(contUpStateMap.containsKey(uploadName))
			{				
				stateObj = contUpStateMap.get(uploadName);
			}
			else
			{
				try
				{
					att = st.execQuery("select * from "+ table+ " where contentid = '"+ uploadName + "' and type = '" + type + "'"); // In case of CUstodian , Service Instatnce, DataServer add this line				
					if(att!=null && att[0].length() != 0)
					{
						System.out.println("In getStateObject's Database attribute Access");
						offset = Integer.parseInt(att[1]);
						preferredInterface = Integer.parseInt(att[2]);
						totalSegments = Integer.parseInt(att[3]);
						currentSegments = Integer.parseInt(att[0]);
						appId = att[4];
						a = Integer.parseInt(att[5]);
						str = att[6]+":"+att[7];
						preferredRoute = new ArrayList<String>();
						preferredRoute.add(str);
						bitMap = new BitSet(totalSegments);
						bitMap.clear();
						bitMap.set(0,currentSegments,true);					 
					}	
				}
				catch(NumberFormatException e){
				}
				stateObj = new ContentState(contentId,uploadName,offset,bitMap,preferredInterface,preferredRoute,totalSegments,currentSegments,stateType,appId,metaDataFlag);
				contUpStateMap.put(uploadName, stateObj);
			}			
		}
		else
		{
			if(contDownStateMap.containsKey(contentId)){
					return contDownStateMap.get(contentId);
			}
			else{
				try
				{
					att = st.execQuery("select * from "+ table+ " where contentid = '"+ contentId + "' and type = '" + type + "'"); // In case of CUstodian , Service Instatnce, DataServer add this line				
					if(att!=null && att[0].length() != 0)
					{
						System.out.println("In getStateObject's Data Access");
						offset = Integer.parseInt(att[1]);
						preferredInterface = Integer.parseInt(att[2]);
						totalSegments = Integer.parseInt(att[3]);
						currentSegments = Integer.parseInt(att[0]);
						appId = att[4];
						a = Integer.parseInt(att[5]);
						str = att[6]+":"+att[7];
						preferredRoute = new ArrayList<String>();
						preferredRoute.add(str);
						bitMap = new BitSet(totalSegments);
						bitMap.clear();
						bitMap.set(0,currentSegments,true);					 
					}	
				}
				catch(NumberFormatException e){
					e.printStackTrace();
				}
				if(offset == -1 && preferredInterface == -1 && preferredRoute == null && totalSegments == -1 && appId == null && !metaDataFlag)
					return null ;
				else{
				stateObj = new ContentState(contentId,offset,bitMap,preferredInterface,preferredRoute,totalSegments,currentSegments,stateType,appId,metaDataFlag);
				contDownStateMap.put(contentId,stateObj);
				}
			}
			
		}

		return stateObj;
	}
	
	public  synchronized List<ContentState> getStateObject(String contentId)///sYNC
	{
		List<ContentState> contList = new ArrayList<ContentState>();
		if(contUpMultiMap.containsKey(contentId)){
			contList = contUpMultiMap.get(contentId);
		}
		return contList ;
		
	}

	public  synchronized List<ContentState> getDtnStateObject(String contentId)///sYNC
	{
		List<ContentState> contList = new ArrayList<ContentState>();
		if(dtnUpMultiMap.containsKey(contentId)){
			contList = dtnUpMultiMap.get(contentId);
		}
		return contList ;
		
	}

	@SuppressWarnings("deprecation")
	/*public  synchronized void setTCPDownloadState(ContentState stateObj)
	{
		setStateObject1(stateObj);
		Status st = Status.getStatus();
		String contentId = stateObj.getContentId();
		List<String> requestfordwn = new ArrayList<String>();
		boolean flag = true;
		requestfordwn = st.getDownloadRequests("downloadrequest");
		if(requestfordwn.size()>=1)
			flag = false ;
		downloadRequest = st.setDownloadRequests("downloadrequest", contentId,0);
		System.out.println("Download request is: "+downloadRequest);
		rmiServer = NewStack.getRMIServer();
		if(downloadRequest.size()==1 && flag)
		{	
			if(rmiServer.isNotRunning()){
				System.out.println("Starting the RMI Server");
				rmiServer.start();
			}
			else{
				rmiServer.setExecute();
				rmiServer.resume();
				System.out.println("Resuming the RMI Server");
			}
		}	
		
	}*/
	
	public  synchronized void setTCPDownloadState(ContentState stateObj)
	{
		setStateObject1(stateObj);
		Status st = Status.getStatus();
		String contentId = stateObj.getContentId();
		downloadRequest = st.setDownloadRequests("downloadrequest", contentId,0);
		System.out.println("Download request is: "+downloadRequest);
		rmiServer = NewStack.getRMIServer();
		if(downloadRequest.size() >= 1)
		{	
			if(rmiServer.isNotRunning()){
				System.out.println("Starting the RMI Server");
				rmiServer.start();
			}
			else{
				rmiServer.setExecute();
				rmiServer.resume();
				System.out.println("Resuming the RMI Server");
			}
		}	
	}
	

	@SuppressWarnings("deprecation")
	public  synchronized void setTCPUploadState(ContentState stateObj, String fileType) 
	{
		setStateObject(stateObj);
		Status st = Status.getStatus();
		String contentId = stateObj.getContentId();
		String uploadId = stateObj.getUploadId();
		requestMap = st.setUploadRequests("uploadrequest",contentId,uploadId,fileType);
		System.out.println("Upload list size is: "+requestMap.size());
		uploadRequest.add(contentId);
		dataUp = NewStack.getDataUploader();
		if(requestMap.size() == 1)
		{
			if(dataUp.isNotRunning()){
				dataUp.start();
				System.out.println("Start The DataUploader");
			}	
			else
			{
				dataUp.setExecute();
				dataUp.resume();
				System.out.println("Resume the DataUploader");
			}
					
		}
		
	}


	public  void setDTNState(ContentState stateObj)
	{
		Properties state = new Properties();
		FileInputStream fis;
		try {
			fis = new FileInputStream(status);
			state.load(fis);
			fis.close();

			String contentId = stateObj.getContentId();
			BitSet bitMap = null ;   // null will cause error allocate memory
			String dtnRequestsString = state.getProperty(dtnData);
			List<String> dtnRequests;
			if(dtnRequestsString != null)
				dtnRequests = Utils.parse(dtnRequestsString);
			else
				dtnRequests = new ArrayList<String>();

			if(!dtnRequests.contains(contentId))
			{
				dtnRequests.add(contentId);
				state.setProperty(dtnData,dtnRequests.toString());
			}

			String uploadId = stateObj.getUploadId();
			state.setProperty(contentId+uploadNameSuffix,uploadId);

			FileOutputStream out = new FileOutputStream(status);
			state.store(out,"--FileUpload&Download Status--");
			out.close();

			ContentState stateObject1 = new ContentState(uploadId,stateObj.getOffset(),bitMap,stateObj.getPreferredInterface(),
					stateObj.getPreferredRoute(),stateObj.getTotalSegments(),stateObj.getCurrentSegments(),stateObj.getStateType(),stateObj.getAppId(),stateObj.getMetaDataFlag());

			setStateObject(stateObject1);

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}catch(IOException e){
			e.printStackTrace();
		}
	}

	@SuppressWarnings("deprecation")
	public  void setDTNDownState(ContentState stateObj)
	{
		setStateObject1(stateObj);
		Status st = Status.getStatus();
		String contentId = stateObj.getContentId();
		//System.out.println("In setTCPDownloadState method");
		List<String> downReq  = new ArrayList<String>();
		boolean flag = true;
		downReq = st.getDownloadRequests("dtndownrequest");
		if(downReq .size()>=1)
			flag = false ;
		dtndownRequest = st.setDownloadRequests("dtndownrequest", contentId,0);
		System.out.println("DTN Download request is: "+dtndownRequest);
		rmiServer = NewStack.getRMIServer();
		if(dtndownRequest.size()==1 && flag && downloadRequest.size()==0)
		{	
			if(rmiServer.isNotRunning()){
				System.out.println("Starting the RMI Server");
				rmiServer.start();
			}
			else{
				rmiServer.setExecute();
				rmiServer.resume();
				System.out.println("Resuming the RMI Server");
			}
		}	

	}
	
	public synchronized void setStateObject(ContentState stateObj)
	{
		Status st = Status.getStatus(); 
		int type = 1 ;
		String contentId = stateObj.getContentId(); 
		ContentState.Type stateType = stateObj.getStateType();
		if(stateType == ContentState.Type.tcpDownload)
			type = 0 ;
		if(type==1 && stateObj.getUploadId() != null)
			contentId = stateObj.getUploadId();
		System.out.println("ContentId and type: "+contentId+" "+type);
					
		String[] con = null ;
		if(stateObj.getPreferredRoute() != null){
				String s1 = stateObj.getPreferredRoute().toString();
				String s2 = s1.substring(1,s1.length()-1);
				con = s2.split(":");						
		}	
		else
		{
			con = new String[2];
			con[0] = "";
			con[1] = "";
		}
			
		int a = 0 ;
		if(stateObj.getMetaDataFlag())
			a=1;
		if(type==0){
			contDownStateMap.put(contentId,stateObj);
			System.out.println("Putting "+contentId+" in contDownStateMap");
		}	
		else
			contUpStateMap.put(contentId, stateObj);
		st.insertData(table,contentId,type,stateObj.getTotalSegments(),stateObj.getCurrentSegments(),stateObj.getOffset(),stateObj.getPreferredInterface(),con[0],stateObj.getAppId(),a,con[1]);
			
	}
		
	public synchronized void setStateObject1(ContentState stateObj) //Modified on 11-04-2011
	{
		Status st = Status.getStatus(); 
		int type = 1 ;
		if(stateObj.getStateType() == ContentState.Type.dtn)
			type = -1 ;
		String contentId = stateObj.getContentId(); 
		System.out.println("ContentId and type: "+contentId+" "+type);
		String[] con = null ;
		if(stateObj.getPreferredRoute() != null){
				String s1 = stateObj.getPreferredRoute().toString();
				String s2 = s1.substring(1,s1.length()-1);
				con = s2.split(":");						
		}	
		else
		{
			con = new String[2];
			con[0] = "";
			con[1] = "";
		}
			
		int a = 0 ;
		if(stateObj.getMetaDataFlag())
			a=1;
		/*******************Added in 11-04-2011*****************/
		if(type == 1){
			if(contUpMultiMap.containsKey(contentId)){
				List<ContentState> contList = contUpMultiMap.get(contentId);
				contList.add(stateObj);
				contUpMultiMap.put(contentId,contList);
			}
			else{
				List<ContentState> contList = new ArrayList<ContentState>();
				contList.add(stateObj);
				contUpMultiMap.put(contentId,contList);
			}
		}
		else{
			if(dtnUpMultiMap.containsKey(contentId)){
				List<ContentState> contList = dtnUpMultiMap.get(contentId);
				contList.add(stateObj);
				dtnUpMultiMap.put(contentId,contList);
			}
			else{
				List<ContentState> contList = new ArrayList<ContentState>();
				contList.add(stateObj);
				dtnUpMultiMap.put(contentId,contList);
			}
		}
		st.insertData(table,contentId,type,stateObj.getTotalSegments(),stateObj.getCurrentSegments(),stateObj.getOffset(),stateObj.getPreferredInterface(),con[0],stateObj.getAppId(),a,con[1]);
			
	}
	public  void setTCPUPloadRequestList(List<String> uploadRequests)
	{
		Status st = Status.getStatus();
		Set<String> request = requestMap.keySet();
		Iterator<String> it = request.iterator();
		while(it.hasNext()){
			String temp = it.next();
			if(!uploadRequest.contains(temp))
				requestMap = st.setUploadRequests("uploadrequest",temp);
		}
		
	}
		
	public void setTCPDownloadRequestList(List<String> requestList){
		downloadRequest = requestList ;
	}
	
	public void setTCPDownloadRequestList(String request){
		Status st = Status.getStatus();
		downloadRequest = st.setDownloadRequests("downloadrequest", request,1);
		System.out.println("downloadRequest is: "+downloadRequest);
	}
	
	public void setDTNDownloadRequestList(String request){
		Status st = Status.getStatus();
		downloadRequest = st.setDownloadRequests("dtndownrequest", request,1);
		System.out.println("DTN downloadRequest is: "+dtndownRequest);
	}
	public  void setDTNRequestList(List<String> dtnRequests)
	{
		Properties state = new Properties();
		FileInputStream fis;
		try {
			fis = new FileInputStream(status);
			state.load(fis);
			fis.close();

			state.setProperty(dtnData,dtnRequests.toString());

			FileOutputStream out = new FileOutputStream(status);
			state.store(out,"--FileUpload&Download Status--");
			out.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}catch(IOException e)
		{
			e.printStackTrace();
		}
	}

	public  void removeTCPUploadState(ContentState stateObj)
	{
		Properties state = new Properties();
		FileInputStream fis;
		try {
			fis = new FileInputStream(status);
			state.load(fis);
			fis.close();


			String uploadRequestsString = state.getProperty(tcpUploadRequest);
			List<String> uploadRequests;
			if(uploadRequestsString != null)
				uploadRequests = Utils.parse(uploadRequestsString);
			else
				uploadRequests = new ArrayList<String>();

			String contentId = stateObj.getContentId();

			if(uploadRequests.contains(contentId))
			{
				uploadRequests.remove(contentId);
				state.setProperty(tcpUploadRequest,uploadRequests.toString());
			}

			FileOutputStream out = new FileOutputStream(status);
			state.store(out,"--FileUpload&Download Status--");
			out.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}catch(IOException e)
		{
			e.printStackTrace();
		}

	}

	public  void removeTCPDownloadState(ContentState stateObj)
	{
		Properties state = new Properties();
		FileInputStream fis;
		try {
			fis = new FileInputStream(status);
			state.load(fis);
			fis.close();


			String downloadRequestsString = state.getProperty(tcpDownloadRequest);
			List<String> downloadRequests;
			if(downloadRequestsString != null)
				downloadRequests = Utils.parse(downloadRequestsString);
			else
				downloadRequests = new ArrayList<String>();

			String contentId = stateObj.getContentId();

			if(downloadRequests.contains(contentId))
			{
				downloadRequests.remove(contentId);
				state.setProperty(tcpDownloadRequest,downloadRequests.toString());
			}

			FileOutputStream out = new FileOutputStream(status);
			state.store(out,"--FileUpload&Download Status--");
			out.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}catch(IOException e)
		{
			e.printStackTrace();
		}

	}

	public  void removeDTNState(ContentState stateObj)
	{
		Properties state = new Properties();
		FileInputStream fis;
		try {
			fis = new FileInputStream(status);
			state.load(fis);
			fis.close();


			String dtnDataString = state.getProperty(dtnData);
			List<String> dtnDataList;
			if(dtnDataString != null)
				dtnDataList = Utils.parse(dtnDataString);
			else
				dtnDataList = new ArrayList<String>();

			String contentId = stateObj.getContentId();

			if(dtnDataList.contains(contentId))
			{
				dtnDataList.remove(contentId);
				state.setProperty(tcpDownloadRequest,dtnDataList.toString());
			}

			FileOutputStream out = new FileOutputStream(status);
			state.store(out,"--FileUpload&Download Status--");
			out.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}catch(IOException e)
		{
			e.printStackTrace();
		}
	}

	/*public  boolean containsTCPDownloadRequest(String dataname) {
		FileInputStream fis;
		Properties state = new Properties();
		try {
			fis = new FileInputStream(status);
			state.load(fis);
			fis.close();

			List<String> downloadRequest;
			String downloadRequestString = state.getProperty(tcpDownloadRequest);
			if(downloadRequestString != null)
				downloadRequest = Utils.parse(downloadRequestString);
			else
				downloadRequest = new ArrayList<String>();

			if(downloadRequest.contains(dataname))
				return true;
			else 
				return false;


		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return false;
		}catch(IOException e)
		{
			e.printStackTrace();
			return false;
		}
	}*/
	public  void uploadStat(String name){
		FileInputStream fis ;
		Properties state = new Properties();
		try{
			fis = new FileInputStream(status);
			state.load(fis) ;
			fis.close();
			List<String> uploadRequest ;
			String uploadRequestString = state.getProperty(tcpUploadRequest);
			System.out.println("Uploaded files name: " + uploadRequestString) ;
			uploadRequest = Utils.parse(uploadRequestString);
			uploadRequest.remove(name);
			System.out.println("upload Request now is : "+ uploadRequest.toString());
			state.setProperty(tcpUploadRequest, uploadRequest.toString());
			FileOutputStream out = new FileOutputStream(status);
			state.store(out,"--FileUpload&Download Status--");
			
		}catch(Exception e){
			
		}
		
	}
	public static Map<String, ContentState> getUpMap(){
	    	return contUpStateMap ;
	}
	public static Map<String, ContentState> getDownMap(){
    		return contDownStateMap ;
    }  
	
	public Map<String,List<ContentState>> getcontUpMultiMap(){
		return contUpMultiMap ;
	}
}




