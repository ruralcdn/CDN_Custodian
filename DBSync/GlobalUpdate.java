package DBSync;
import java.net.Socket;
import java.sql.*;
import java.util.concurrent.BlockingQueue;
import java.io.*;

public class GlobalUpdate extends Thread {
	BlockingQueue<Socket> clsocks ;
	Socket sock;
	Connection con ;
	Statement stmt ;
	ResultSet rs ;
		
	public GlobalUpdate(BlockingQueue<Socket> clientSocks){
		try{
 			Class.forName("com.mysql.jdbc.Driver");
 			con = DriverManager.getConnection
				("jdbc:mysql://localhost:3306/syncdb","root","abc123");
 			clsocks = clientSocks ;
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
		
	public void run()
	{
		while(true){
			try {
				sock =clsocks.take();
				String client = sock.getInetAddress().getHostName();
				if(client.equals("localhost"))
				{
					ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());
					String query = (String) ois.readObject();
					System.out.println("Client Host Name is: "+client);
					stmt = con.createStatement();
	 				stmt.execute(query);
	 				File outFile = new File("cache_db.log");
	 				BufferedWriter writer = new BufferedWriter(new FileWriter(outFile,true));
	 				writer.write(query);
	 				writer.newLine();
	 				writer.close();
	 				sock.close();
	 			}
				else
				{
					System.out.println("Client Host Name is: "+client);
					OutputStream ous = sock.getOutputStream();
 	 				InputStream  ois = sock.getInputStream();
					logFileRead(ois);
					stmt = con.createStatement();
					stmt.execute("select updated_till from synctable where entity ='"+client+"'");
					rs = stmt.getResultSet();
					int cp = 0 ;
					if(rs.next())
						cp = rs.getInt(1);
					rs.close();
					System.out.println("Value of cp before updating is : "+cp);
					logFileSent(ous, cp);
					ois.close();
					ous.close();
					executeLogStatement();
					updateLogFile("download.log","cache_db.log", client);
					File logFile = new File("download.log");
					if(logFile.exists())
						logFile.delete();
					sock.close();
				}	
				/*stmt.execute("select * from synctable where entity !='"+client+"'");
				rs = stmt.getResultSet();
				while(rs.next())
				{
					int current = rs.getInt(2);
 					String serverName = rs.getString(1);
 					//System.out.println("In SyncServer entity: "+serverName+" with updated_till value: "+current);
 					InetAddress server = InetAddress.getByName(serverName);
 	 				InetAddress host = InetAddress.getByName("user1");
 	 				
 	 				/*****************Added 08/04/2011 begins************************************************/
 	 			/*	Iterator<Socket> socks = clsocks.iterator();
 	 				while(socks.hasNext()){
 	 					Socket temp = socks.next();
 	 					if(temp.getInetAddress().equals(server))
 	 						continue;
 	 					
 	 				}
 	 				/*****************Added 08/04/2011 ends*****************************************************************/
 	 				/*Socket clientSocket = new Socket(server, 6789,host,0);
 	 				OutputStream ous1 = clientSocket.getOutputStream();
 	 				InputStream  ois1 = clientSocket.getInputStream();
 	 				logFileSent(ous1,current);
 	 				logFileRead(ois1);
 	 				executeLogStatement();
					updateLogFile("download.log","cache_db.log", serverName);
					File logFile = new File("download.log");
					if(logFile.exists())
						logFile.delete();
					ois1.close();
					ous1.close();
					clientSocket.close();
				}*/
				
				} 
			//}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
	
	public void logFileRead(InputStream ois) throws Exception{
		File logFile = new File("download.log");
		int bytesRead ;
		int current = 0 ;
		byte[] data = new byte[10240];
		FileOutputStream fos = new FileOutputStream(logFile);
		BufferedOutputStream bos = new BufferedOutputStream(fos);
		bytesRead = ois.read(data,0,data.length);
		current = bytesRead ;
		bos.write(data,0,current);
		bos.close();
		fos.close();
		
	}
	
	public void logFileSent(OutputStream ous, int cp)throws Exception{
		File toBeRead = new File("cache_db.log");
		File toBeSent = new File("upload.log");
		BufferedReader reader = new BufferedReader(new FileReader(toBeRead)); 
		BufferedWriter writer = new BufferedWriter(new FileWriter(toBeSent));
		String str;
		int count = 0 ;
		boolean blank = true ;
		while((str = reader.readLine())!=null){
			++count ;
			if(count<=cp){
				continue ;
			}	
			else{
				writer.write(str);
				writer.newLine();
				blank = false ;
			}
		}
		if(blank)
			writer.newLine();
		reader.close();
		writer.close();
		byte[] data = new byte[(int) toBeSent.length()];
		FileInputStream fis = new FileInputStream(toBeSent);
		BufferedInputStream bis = new BufferedInputStream(fis);
		bis.read(data,0,data.length);
		ous.write(data,0,data.length);
		ous.flush();
		bis.close();
		fis.close();
		System.out.println("LogFile has been Sent");
		if(toBeSent.exists())
			toBeSent.delete();
		
	}
	
	
	public void executeLogStatement() throws Exception{
		File logFile = new File("download.log");
		BufferedReader reader = new BufferedReader(new FileReader(logFile)); 
		String str ;
		Statement stmt = con.createStatement();
		while((str=reader.readLine())!=null && str.length()>1){
			stmt.execute(str);
		}
		reader.close();
	}
	
	
	public void updateLogFile(String downloadFile, String logFile, String Client){
		try{
			File temp = new File(downloadFile);
			File cache_log = new File(logFile);
			BufferedReader reader = new BufferedReader(new FileReader(temp)); 
			BufferedWriter writer = new BufferedWriter(new FileWriter(cache_log,true));
			String str;
			while((str = reader.readLine())!=null && str.length()!=0){
				writer.write(str);
				writer.newLine();
			}
			reader.close();
			writer.close();
			BufferedReader reader1 = new BufferedReader(new FileReader(cache_log));
			int count = 0 ;
			while(reader1.readLine()!= null)
				++count ;
			reader1.close();
			Statement stmt = con.createStatement();
			stmt.execute("update synctable set updated_till ="+count+" where entity ='"+Client+"'");
			stmt.close();
			System.out.println("Value of cp after updating: "+count);
			
		}catch(Exception e)
		{
			e.printStackTrace();
		}
		
	}

}
