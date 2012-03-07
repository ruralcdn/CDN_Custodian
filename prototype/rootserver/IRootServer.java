package prototype.rootserver;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface IRootServer extends Remote {
	public boolean register(String dataname,String source) throws RemoteException;
	public boolean deregister(String dataname,String source) throws RemoteException;
	public List<String> find(String dataname) throws RemoteException;
	
}
