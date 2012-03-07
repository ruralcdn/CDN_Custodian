package prototype.serviceinstance;

import java.rmi.Remote;
import java.rmi.RemoteException;

import newNetwork.Connection;

public interface IUploader extends Remote{

	//public void notify_upload(String contentId) throws RemoteException;
	public String upload(String data,int size,String requester) throws RemoteException;
	public int processDynamicContent(String objectId,int uploadSize,String contentId,Connection.Type type,String requester,String conId) throws RemoteException;
	public String generateContentId() throws RemoteException ;

}