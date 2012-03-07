package prototype.user;
import java.rmi.*;
import PubSubModule.Notification;
public interface IAppFetcher extends Remote {
	
	public void uploadNotify(Notification notificationlist) throws RemoteException;

	public void uploadLog() throws RemoteException;

}
