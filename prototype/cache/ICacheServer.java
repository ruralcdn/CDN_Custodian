package prototype.cache;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import newNetwork.Connection;

public interface ICacheServer extends Remote{
	
	public void notify(String data) throws RemoteException; 
	//public String upload(String data,int size,String destination, String userId) throws RemoteException;
	public String upload(String data,int size,String destination, String userId, String fileType) throws RemoteException;
	public  String dtnUpload(String userContentName,int size,String dest, String userId, String fileType) throws RemoteException;
	public long find(int AppId,String dataname,Connection.Type type, String dest) throws RemoteException;
	public List<Integer> getUploadAcks(String ContentName, int size) throws RemoteException ;
}