package StateManagement;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

public class ServiceInstanceAppStateManager{

	File status;
    FileInputStream fis ;
	public static final String uploadRequesterSuffix = new String(".UploadRequester");

	//public ServiceInstanceAppStateManager(File state)
	public ServiceInstanceAppStateManager()
	{
		//status = state;
	}

	public synchronized String getUploadRequester(String contentId)
	{
		try {

			Properties state = new Properties();
			//FileInputStream fis;

			fis = new FileInputStream(status);
			synchronized(fis){state.load(fis);
			fis.close();}
			
			return state.getProperty(contentId+uploadRequesterSuffix);
		
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}catch(IOException e)
		{
			e.printStackTrace();
			return null;
		}
	}
	
/*	public String getDownloadName(String contentId)
	{
		try {

			Properties state = new Properties();
			FileInputStream fis;

			fis = new FileInputStream(status);
			state.load(fis);
			fis.close();
			
			return state.getProperty(contentId+downloadNameSuffix);
		
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}catch(IOException e)
		{
			e.printStackTrace();
			return null;
		}
	}
*/
	public synchronized void setRequesterDetail(String uploadId,/*String downloadId,*/String requesterId)
	{
		try {

			Properties state = new Properties();
			//FileInputStream fis;

			fis = new FileInputStream(status);
			synchronized(fis){state.load(fis);
			fis.close();}
			
			//state.setProperty(uploadId+downloadNameSuffix,downloadId);
			state.setProperty(uploadId+uploadRequesterSuffix,requesterId);
			
			FileOutputStream out = new FileOutputStream(status);
			state.store(out,"--FileUpload&Downloadstatu--");
			out.close();
		
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch(IOException e)
		{
			e.printStackTrace();
		}
	}
	

}