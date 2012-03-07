package prototype.dbserver;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface IDBServer extends Remote{
	public boolean upload(String contentId, int segments, String dest) throws RemoteException ;
	public int find(String contentName, String dest) throws RemoteException;
	public void logFileToSend(String dest)throws Exception ;
	public void executeLog(String dest) throws RemoteException ;
	public void executeLogStatement() throws Exception;
	public void updateLogFile(String downFile, String logFile, String Client) throws Exception ;
	public List<Integer> getUploadAcks(String contentName, int size) throws RemoteException ;
	public void infoIP(String userId, String IPAdd) throws RemoteException ;
	public boolean uploadThumb(String contentName, int size) throws RemoteException ;
	public int findImg(String imgName, String userdaemon) throws RemoteException ;
}