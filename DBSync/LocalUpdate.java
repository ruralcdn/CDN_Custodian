package DBSync;

import java.io.*;
import java.net.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class LocalUpdate extends Thread
{
	public static BlockingQueue<String> insMap ;
	
	public LocalUpdate(){
		insMap = new ArrayBlockingQueue<String>(20);
	}
 	
	public void run()
 	{
 		System.out.println("Starting the DBClient");
 		while(true)
 		{
 			try {
 				System.out.println("Here in LocalUpdate with insMap size: "+insMap.size());
 				String query = insMap.take();
 				Socket sock = new Socket("localhost",6789);
 				ObjectOutputStream outToClient = new ObjectOutputStream(sock.getOutputStream());
	 			outToClient.writeObject(query);
	 			sock.close();
	 			
 			}catch (Exception e) {
 				e.printStackTrace();
 			} 
 		}
 	}	
 	
}
