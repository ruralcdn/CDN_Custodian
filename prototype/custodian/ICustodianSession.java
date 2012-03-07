package prototype.custodian;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import PubSubModule.Notification;
import newNetwork.Connection;

public interface ICustodianSession extends Remote {	
	public boolean subscribe(String subject) throws RemoteException;
	public boolean unsubscribe(String subject) throws RemoteException;
	public boolean close_connection() throws RemoteException;
	public void upload(String data,int size) throws RemoteException;
	public  String upload(String userContentName,int size,String dest, String userId, String fileType) throws RemoteException;
	public  String DTNUpload(String userContentName,int size,int tcpSize,String dest, String userId, String fileType) throws RemoteException;	
	public List<Notification> poll_notification() throws RemoteException;
	public List<String> processDynamicContent(String uploadContentId,int  uploadSize,String replyContentId,Connection.Type uploadType,Connection.Type downloadType,String dest) throws RemoteException;
	//public long find(int AppId,String dataname,Connection.Type type, String dest) throws RemoteException;
	public long find(int AppId,String dataname,Connection.Type type, String dest,int ch) throws RemoteException;
	public boolean delete(String ContentId) throws RemoteException ;
	//public int request_data(String data) throws RemoteException;
	//public List<String> DTNUpload(String data,int size,String dest) throws RemoteException;
	//public String upload(String data,int size,String destination) throws RemoteException;
	//returns the location destination for the content which is used as destination in the content objects
	//public String DTNUpload(String data,int size) throws RemoteException;
}
