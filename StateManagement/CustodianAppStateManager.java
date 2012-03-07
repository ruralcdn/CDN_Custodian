package StateManagement;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import prototype.utils.Utils;

public class CustodianAppStateManager{

	File status;
    FileInputStream fis ;
	public static final String uploadNotificationSuffix = new String(".UploadNotifications");

	//public CustodianAppStateManager(File state)
	public CustodianAppStateManager()
	{
		//status = state;
	}

	/*public synchronized void setUploadNotification(String userId,List<String> notificationList)
	{
		try {

			Properties state = new Properties();
			FileInputStream fis = new FileInputStream(status);
			state.load(fis);
			fis.close();
			
			state.setProperty(userId+uploadNotificationSuffix,notificationList.toString());
		
			FileOutputStream out = new FileOutputStream(status);
			state.store(out,"---No Comments1dsdsds");
			out.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}catch(IOException e)
		{
			e.printStackTrace();
		}
	}*/
	
	public synchronized void setUploadNotification(String userId,String notificationList){
		Status st = Status.getStatus();
		st.setuploadNotification("userupload", userId, notificationList);
		
	}
	
	public synchronized List<String> getUploadNotification(String userId)
	{

		try {

			Properties state = new Properties();
			//FileInputStream fis;

			fis = new FileInputStream(status);
			synchronized(fis){state.load(fis);
			fis.close();}
			
			List<String> uploadList;
			String uploadNotifications =  state.getProperty(userId+uploadNotificationSuffix);
			if(uploadNotifications == null)
				uploadList = new ArrayList<String>();
			else
				uploadList = Utils.parse(uploadNotifications);
			
			return uploadList;
			
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}catch(IOException e)
		{
			e.printStackTrace();
			return null;
		}
	}
}