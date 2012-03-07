package prototype.rendezvous;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.NotBoundException;
import java.util.List;

public interface IRendezvous extends Remote {
	public List<String> find(String dataname) throws RemoteException,NotBoundException;

}