package PubSubModule;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import prototype.user.IAppFetcher;

import StateManagement.CustodianAppStateManager;
import StateManagement.Status;


public class PubSubNode implements IPubSubNode{
	
	CustodianAppStateManager appStateManager;
	List<PubSubNode> nodes;
	
	public PubSubNode(CustodianAppStateManager manager,List<PubSubNode> nodeList)
	{	
		appStateManager = manager;
		nodes = nodeList;
	}
	
	public void notify(Notification notification) throws RemoteException
	{
		System.out.println("Call received from rootServer");
		if(nodes == null && appStateManager != null)
		{
			if(notification.getNotificationType() == Notification.Type.UploadAck)
			{
				boolean flag = false ;
				while(!flag){
					try
					{
						String[] notificationContent = notification.getContent().split(":");
						appStateManager.setUploadNotification(notificationContent[0], notificationContent[1] );
						Status st = Status.getStatus();
						String usernode = st.getUserLocation(notificationContent[0]);
                                                
						Registry userRegistry = LocateRegistry.getRegistry(usernode);
						System.out.println("");
						System.out.println("userNode = " + usernode);
						System.out.println("");
						
						IAppFetcher appFetcherStub = (IAppFetcher) userRegistry.lookup("appfetcher");
						System.out.println("In pubSubNode, value of appFetcher: "+appFetcherStub);//to be commented
						Notification notif = new Notification(Notification.Type.UploadAck,notificationContent[1]);
						appFetcherStub.uploadNotify(notif);
						flag = true ;

					}catch(Exception e){
						try {
							e.printStackTrace();
							Thread.sleep(2000);
						} catch (InterruptedException e1) {

						}
					}
				}
			}
		}

	}
	
}