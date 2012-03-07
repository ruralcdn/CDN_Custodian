package prototype.user;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.Scanner;

import newNetwork.Connection;

public class Client{

	public static void main(String[] args) {

		String host = new String("localhost");
		String username ;
		String password ;
		Scanner in = new Scanner(System.in);
		
		
		try {
			
			/****************** RMI Registry with userdaemon ******************/
			
			Registry registry = LocateRegistry.getRegistry(host);
			IUser stub = (IUser) registry.lookup("userdaemon");
			
			
			/*********************User Login*********************************************/
			System.out.println("Enter the username: ") ;
			username = in.nextLine();
			System.out.println("Enter the password: ") ;
			password = in.nextLine();
			stub.login(username,password);
			
								
			/*******************Uploading Files**********************/
			
			System.out.println("Requesting for upload in Client.java");
			System.out.println("Enter the File name to be uploaded:") ;
			String name = in.nextLine();
			int appId = stub.getAppId();
			String contentId = stub.upload(name,Connection.Type.DSL,appId,"youtube.com");
			System.out.println("ContentId generated is : "+contentId);
			List<String> uploadList = null;
			boolean execute = true;
			while(execute)
			{
				uploadList = stub.getUploadList();
				for(int i = 0 ; i < uploadList.size() ;){
					if(contentId.equals(uploadList.get(i++))){
						execute = false ;
						stub.uploadStatus(name) ;
					    break ;
					}   
				}
			} 
			System.out.println("The following files have been uploaded: "+uploadList.toString());
			
		    
			/***************** Downloading Files*****************/
			
			stub.find(contentId,Connection.Type.DSL,1); 
			List<String> downloadList = null;
			boolean execute1 = true;
			while(execute1)
			{
				downloadList = stub.getDownloadList(1);
				if(!downloadList.isEmpty())
					execute1 = false;
			}
			System.out.println("The Following files have been downloaded: "+downloadList.toString());
            
           
			try {	
				Thread.currentThread();
				Thread.sleep(3*600000);
				stub.logout();	
				System.out.println("logged Out");

			} catch (Exception e) {
				System.err.println("Client exception: " + e.getMessage());
				e.printStackTrace();
			}

		} catch (Exception e) {
			System.err.println("Client exception: " + e.toString());
			e.printStackTrace();
		}
	}
}