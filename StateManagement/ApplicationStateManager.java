package StateManagement;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


import prototype.utils.Utils;

public class ApplicationStateManager{

	private static final String uploadAcksSuffix = new String(".UploadAcks");
	private static final String userUploadIdSuffix = new String(".UserUploadId");
	//private boolean available = false ;
	File status;
    public ApplicationStateManager(File state)
	{
		status = state;	
	}

    
	public void setServiceUploadName(String uploadId,String serviceUploadId)///sync
	{
		Properties state = new Properties();
		FileInputStream fis;
		try {
			fis = new FileInputStream(status);
			state.load(fis);
			fis.close();
			
			state.setProperty(serviceUploadId+userUploadIdSuffix,uploadId);
			
			FileOutputStream out = new FileOutputStream(status);
			state.store(out,"--FileUpload&DownloadStatus--");
			out.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	
	public String getUserUploadId(String serviceUploadId)///sync
	{
		Properties state = new Properties();
		FileInputStream fis;
		
		try {
			fis = new FileInputStream(status);
			state.load(fis);
			fis.close();
		    return state.getProperty(serviceUploadId+userUploadIdSuffix);
		
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}catch(IOException e){
			e.printStackTrace();
			return null;
		}
	}

	
	public List<String> getUploadAcks(int AppId)///sync
	{
		Properties state = new Properties();
		FileInputStream fis;
		try {
			fis = new FileInputStream(status);
			state.load(fis);
			fis.close();			
			List<String> UploadAcks;
			String uploadAcksString = state.getProperty(AppId+uploadAcksSuffix);
			if(uploadAcksString == null)
				UploadAcks = new ArrayList<String>();
			else
				UploadAcks = Utils.parse(uploadAcksString);

			return UploadAcks;
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}catch(IOException e){
			e.printStackTrace();
			return null;
		}
		

	}
	
	
	public void addUploadAcks(String AppId,String contentId)///sync
	{

		Properties state = new Properties();
		FileInputStream fis;
		try {
			fis = new FileInputStream(status);
			state.load(fis);
			fis.close();

			List<String> UploadAcks;
			String uploadAcksString = state.getProperty(AppId+uploadAcksSuffix);
			if(uploadAcksString == null)
				UploadAcks = new ArrayList<String>();
			else
				UploadAcks = Utils.parse(uploadAcksString);
			
			UploadAcks.add(contentId);
			state.setProperty(AppId+uploadAcksSuffix, UploadAcks.toString());
			FileOutputStream out = new FileOutputStream(status);
			state.store(out,"--FileUpload&DownloadStatus--");
			out.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}catch(IOException e){
			e.printStackTrace();
		}

	}

}