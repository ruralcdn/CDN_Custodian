package prototype.dataserver;

import java.rmi.Remote;
import java.rmi.RemoteException;

import newNetwork.Connection;

public interface IDataServer extends Remote {
	
	public void upload(String data,int size,String requester, String fileType) throws RemoteException;
	public void upload(String data,String requester, String fileType) throws RemoteException;//only for usb
	public int TCPRead(int AppId,String dataname,Connection.Type type,String conId) throws RemoteException;
	//this method uses point to point connection for DTN data transfer 
	public boolean DTNRead(int AppId,String dataname,String dataRequester,String conId) throws RemoteException;
	public boolean delete(String contentId) throws RemoteException;
	public boolean upload(String string, int segments, String dbServerId) throws RemoteException;
	public int findLog(String string, String string2) throws RemoteException;
	
}
