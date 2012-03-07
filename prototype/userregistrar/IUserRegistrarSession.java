package prototype.userregistrar;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface IUserRegistrarSession extends Remote {	

	public boolean close_connection() throws RemoteException;
	public List<String> get_custodians() throws RemoteException;
	
}