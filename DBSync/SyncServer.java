package DBSync;

import java.net.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class SyncServer extends Thread
{
  BlockingQueue<Socket> clientSocks ;	
  ServerSocket welcomeSocket ;
  GlobalUpdate recCon;
  
   public SyncServer( int port){
	   try{ 
	   	  welcomeSocket = new ServerSocket(port);
	   	  clientSocks = new ArrayBlockingQueue<Socket>(20);
	   }
	   catch(Exception e)
	   {
		  e.printStackTrace(); 
	   }
	  
	   
	   recCon = new GlobalUpdate(clientSocks);
	   recCon.start();
   }
   public void run()
   {
	  System.out.println("Starting the DBServer");
	  while(true){
		  try{
			Socket connectionSocket = welcomeSocket.accept();
		    clientSocks.put(connectionSocket);
		  }
		  catch(Exception e)
		  {
			e.printStackTrace();  
		  }
	   }
   }
   public BlockingQueue<Socket> getClientSocks()
   {
	   return clientSocks ;
   }
}
