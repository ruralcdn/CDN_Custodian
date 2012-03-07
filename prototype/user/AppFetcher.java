/*package prototype.user;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import PubSubModule.Notification;
import StateManagement.ApplicationStateManager;
import StateManagement.ContentState;
import StateManagement.StateManager;
import prototype.custodian.ICustodianSession;


public class AppFetcher extends Thread{

	ICustodianSession session;
	ApplicationStateManager AppStateManager;
	StateManager stateManager;
	boolean execute;

	public AppFetcher(ApplicationStateManager appManager,StateManager manager,ICustodianSession custodianSession) throws IOException,FileNotFoundException
	{
		execute = true;
		AppStateManager = appManager;
		stateManager = manager;
		session = custodianSession;
	}

	public void close()
	{
			System.out.println("closing connection");
			execute = false;
	}

	public void run()
	{
		while(execute)
		{
		//	System.out.println("Requesting Data");		
			try
			{
				if(session == null)
					break;
							
				//update state with notifications it received
				List<Notification> notificationList  = session.poll_notification();
				Iterator<Notification> it1= notificationList.iterator();
				while(it1.hasNext())
				{
					Notification notif = it1.next();
					if(notif.getNotificationType() == Notification.Type.UploadAck)
					{
						String contentId = notif.getContent();
						String userContentId = AppStateManager.getUserUploadId(contentId);
						
						if(contentId != null && !contentId.isEmpty())
						{
						ContentState stateObject = stateManager.getStateObject(userContentId,ContentState.Type.tcpUpload);
						String AppId = stateObject.getAppId();
						AppStateManager.addUploadAcks(AppId, contentId);
						
						}
					}
				}

			}catch(Exception e)
			{
				e.printStackTrace();
			}
			try
			{
				Thread.sleep(10*1000);
			}catch(InterruptedException e)
			{
				e.printStackTrace();
			}
		}
	}
}*/


package prototype.user;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.RemoteException;
import PubSubModule.Notification;
import StateManagement.ApplicationStateManager;
import StateManagement.ContentState;
import StateManagement.StateManager;
import prototype.custodian.ICustodianSession;


public class AppFetcher implements IAppFetcher{

	ICustodianSession session;
	ApplicationStateManager AppStateManager;
	StateManager stateManager;
	boolean execute;

	public AppFetcher(ApplicationStateManager appManager,StateManager manager,ICustodianSession custodianSession) throws IOException,FileNotFoundException
	{
		execute = true;
		AppStateManager = appManager;
		stateManager = manager;
		session = custodianSession;
	}

	public void uploadNotify(Notification notification)throws RemoteException{
		try
		{
				if(notification.getNotificationType() == Notification.Type.UploadAck)
				{
					String contentId = notification.getContent();
					String userContentId = AppStateManager.getUserUploadId(contentId);
					
					if(contentId != null && !contentId.isEmpty())
					{
					ContentState stateObject = stateManager.getStateObject(userContentId,ContentState.Type.tcpUpload);
					String AppId = stateObject.getAppId();
					AppStateManager.addUploadAcks(AppId, contentId);
					
					}
				}
			

		}catch(Exception e){
			
		}
		
	}

	@Override
	public void uploadLog() throws RemoteException {
		// TODO Auto-generated method stub
		
	}
}