package PubSubModule;


import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IPubSubNode extends Remote {
	
	public void notify(Notification notification) throws RemoteException;

}