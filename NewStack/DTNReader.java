package NewStack;

import java.awt.Image;
import java.awt.SystemTray;
import java.awt.Toolkit;
import java.awt.TrayIcon;
import java.io.*;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.BlockingQueue;

import AbstractAppConfig.AppConfig;
import StateManagement.ContentState;
import StateManagement.StateManager;
import prototype.datastore.DataStore;
import java.net.*;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import java.io.FileInputStream;
import java.io.IOException;

//import javax.swing.* ;


public class DTNReader extends Thread {
	String osName ;
	String dirPathinLin ;
	List <String> filesinLin ;
	List<String> readingList ;
	DataStore store ;
	StateManager stateManager;
	int segmentSize;
	Map<String, ContentState> mpContent;
	String[] letters = new String[]{ "A", "B", "C", "D", "E", "F", "G", "H", "I", 
			"J", "K", "L", "M", "N", "O", "P", "Q", "R",
			"S", "T", "U", "V", "W", "X", "Y", "Z" };
	File[] drives = new File[letters.length];

	boolean[] isDrive = new boolean[letters.length];



	public DTNReader(List<String> dtnReadList, StateManager stmgr,DataStore dstore, int segment, BlockingQueue<String> downloads){
		osName= System.getProperty("os.name");
		readingList = dtnReadList ;
		store = dstore;
		stateManager = stmgr ;
		segmentSize = segment ;
		mpContent = new HashMap<String, ContentState>();
		for (int i = 0; i < letters.length; ++i )
		{
			drives[i] = new File(letters[i]+":/");
			isDrive[i] = drives[i].canRead();
		}

		//System.out.println("Os Name is: "+osName);
		if(osName.contains("Linux")){
			dirPathinLin = "/media/";
			filesinLin = new ArrayList<String>();
			File dir = new File(dirPathinLin);

			String[] fileList = dir.list();
			for(int i = 0 ; i < fileList.length; i++){
				filesinLin.add(fileList[i]);
			}
		}
	}

	public void run(){
		while(true)
		{
			try {
				Thread.sleep(1000);// previous value 2000
			} catch (Exception e) {

			}
			if(osName.contains("Windows")){
				for (int i = 0; i < letters.length; ++i){
					boolean pluggedIn = drives[i].canRead();
					if (pluggedIn != isDrive[i]){
						if(pluggedIn)
						{	                
							System.out.println("Drive "+letters[i]+" has been plugged in");
							String str = "cmd /c \"dir "+ letters[i] + ":\"";
							try{
								Process process =Runtime.getRuntime().exec(str);
								BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
								boolean file = false;
								if(findInUSB(input,file,"DTNRouter"))
								{
									System.out.println("The USB key is a DTNRouter.");
								}
								else{
									System.out.println("The USB key is not a DTNRouter.");
									continue;
								}
								/*String filePathStr;
								while(readingList.size()>0)
								{
									String fileName = readingList.remove(0) ;
									filePathStr = letters[i] + ":\\DTNRouter\\"+fileName;
									System.out.println("File Name is: "+filePathStr);
									if(dtnFileRead(filePathStr)){
										System.out.println("File is successfully read");
									}	
									else{
										readingList.add(readingList.size(),fileName);
										try {
											Thread.sleep(2000);
										} catch (Exception e) {
											e.printStackTrace();
										}
									}	
									System.out.println("Reading List size is: "+readingList.size());
								}*/
								String filePathStr = letters[i] + ":\\DTNRouter\\";
								File dtnDir = new File(filePathStr);
								String[] dataList = dtnDir.list(); //List of contents of USB Drive.
								
								//System.out.println("Contents of ReadingList = "+readingList.get(j))
								for(int j = 0; j < dataList.length ; j++)
								{
									for(int k =0;k<readingList.size();k++) 
									{
										//System.out.println("111. dataList = " + dataList[j]);
										
										String fileNameExt = dataList[j].substring(dataList[j].lastIndexOf("."), dataList[j].length()); //ext of USB Drive contents.
										
										dataList[j] = dataList[j].substring(0, dataList[j].lastIndexOf("."));
										//if( k == 0)
										//readingList.set(k,readingList.get(k)+fileNameExt); 
										//System.out.println("ReadingList = " + readingList);
										System.out.println("ReadingList = " + readingList.get(k));
										//System.out.println("222. dataList = " + dataList[j]);
										if(readingList.get(k).contains(dataList[j]))
										{
											String fileName = readingList.remove(0) ;										
											filePathStr = null;
											filePathStr = letters[i] + ":\\DTNRouter\\"+fileName + fileNameExt;
											
											System.out.println("File Name is: "+filePathStr);
											if(dtnFileRead(filePathStr))
											{
												System.out.println("File is successfully read");
												readingList.remove(dataList[j]);
											}	
											else
											{
												//System.out.println("catch File Name is: "+filePathStr);
												try {
													Thread.sleep(1000);//changed 2000 TO 1000
												} catch (Exception e) {
													e.printStackTrace();
												}
											}	
											//System.out.println("Reading List size is: "+readingList.size());
										}
									
									}
								}
								/*System.out.println("Now you can remove your drive");	
								JFrame parent = new JFrame();

							    JOptionPane.showMessageDialog(parent, "NSafely Remove");*/
							} 
							catch (IOException e) 
							{
								//System.out.println("Error in reading or writing file");
								try {
									Thread.sleep(5000);
								} catch (InterruptedException e1) {
									e1.printStackTrace();
								}
							}							
							//JOptionPane.showMessageDialog(null, "Safely Remove USBdrive");
						}
						else
						{
							System.out.println("Drive "+letters[i]+" has been unplugged");
						}
						isDrive[i] = pluggedIn;
					}
				}

				//System.err.println("1");
			}else{

				File dir = new File(dirPathinLin);
				String newDev = "";
				String remDev = "";
				String[] filesTemp = dir.list();
				List<String> temp = new ArrayList<String>();
				for(int i = 0 ; i < filesTemp.length ; i++){
					temp.add(filesTemp[i]);
				}
				if(temp.size() == filesinLin.size()){
					continue ;
				}	
				if(temp.size() > filesinLin.size()){
					for(int i = 0 ; i  < temp.size();i++){
						if(!filesinLin.contains(temp.get(i))){
							newDev = temp.get(i);
							break ;
						}	

					}
					if(readingList.size() == 0)
						break ;
					filesinLin = temp ;
					System.out.println("Plugged Device: "+newDev);
					String newDevdir = dirPathinLin+newDev+"/";
					System.out.println("In DTNReader, value of newDevDir: "+newDevdir);
					try{
						if(findInUSBinLin(newDevdir)){
							System.out.println("The device is DTNRouter");
							newDevdir += "DTNRouter/"; 
						}
						else{
							System.out.println("The device is not  DTNRouter");
							continue ;
						}	
						System.out.println("Reading list size is: "+readingList.size());
						File dtnDir = new File(newDevdir);
						String[] dataList = dtnDir.list();
						for(int i = 0 ; i < readingList.size(); i++){

						}
						String filePathStr;
						for(int i = 0; i < dataList.length ; i++)
						{
							if(readingList.contains(dataList[i])){
								//String fileName = readingList.remove(0) ;
								String fileName = dataList[i];
								filePathStr = newDevdir+fileName;
								System.out.println("File Name is: "+filePathStr);
								if(dtnFileRead(filePathStr)){
									System.out.println("File is successfully read");
									readingList.remove(dataList[i]);
								}	
								else{
									//readingList.add(readingList.size(),fileName);
									try {
										Thread.sleep(2000);
									} catch (Exception e) {
										e.printStackTrace();
									}
								}	
								//System.out.println("Reading List size is: "+readingList.size());
							}
						}
					}catch(Exception e){
						e.printStackTrace();
					}

				}
				else{
					for(int i = 0 ; i  < filesinLin.size();i++){
						if(!temp.contains(filesinLin.get(i))){
							remDev = filesinLin.get(i);
							break ;
						}	
					}
					System.out.println("UnPlugged Device: "+remDev);
					filesinLin = temp ;
				}
			}

		}

	}


	private  boolean findInUSB(BufferedReader input,boolean file,String name) throws IOException
	{
		String line = "";
		do
		{
			line = input.readLine();			
			if(line == null)
			{
				break;
			}
			if(line.contains("DTNRouter"))
			{
				if(line.contains("<DIR>"))
				{					
					return true;
				}				
			}		
		}while(line!=null);						

		return false;    	
	}

	private  boolean findInUSBinLin(String newDevdir)
	{
		try{
			File dir = new File(newDevdir);
			String[] fileList = dir.list();
			for(int i = 0 ; i < fileList.length; i++){
				if(fileList[i].equals("DTNRouter"))
					return true ;
			}
		}catch(Exception e){
			e.printStackTrace();
			return false ;
		}
		return false ;  	
	}

	//commented by amit

	private boolean dtnFileRead(String filePathStr) throws IOException{
		
		boolean flag =  false;
		System.out.println("Ready to move file");
		System.out.println("filepathstr:"+filePathStr);
		int lastIndex = filePathStr.lastIndexOf("\\");
		String fname = "";
		fname = filePathStr.substring(lastIndex+1, filePathStr.length());	
		
		
		System.out.println("Ready to move file:"+fname);
		FileChannel source = null;
		FileChannel destination = null;

		File configFile = new File("config/Custodian.cfg");
		FileInputStream fconfig;
		fconfig = new FileInputStream(configFile);
		new AppConfig();
		AppConfig.load(fconfig);
		fconfig.close();	
		
		String Dir = AppConfig.getProperty("Custodian.Directory.Path");

		try{
			source = new FileInputStream(filePathStr).getChannel();
			destination = new FileOutputStream(Dir+fname).getChannel();
			try{
				destination.transferFrom(source, 0, source.size());
				
				flag = true;
			}catch(Exception ex){
				ex.printStackTrace();
			}
		}
		finally{
			if(source != null){
				source.close();
			}
			if(destination != null){
				destination.close();
			}
		}		

		File afile = new File(filePathStr);
		afile.delete();//delete file form usb

		Toolkit toolkit = Toolkit.getDefaultToolkit();
		Image image = toolkit.getImage(AppConfig.getProperty("Custodian.Usb.Image.Path"));//
		TrayIcon trayIcon = new TrayIcon(image, "TrayIcon");

		SystemTray tray = SystemTray.getSystemTray();
		try
		{
			tray.add(trayIcon);
		}
		catch (Exception e)
		{
			System.err.println("TrayIcon could not be added.");							           
		}

		trayIcon.displayMessage("Remove USB", "Please Safely remove your usb Drive", TrayIcon.MessageType.INFO);

		/**
		 * code for uploading the file using ftp	
		 */
		FTPClient client = new FTPClient();
		FileInputStream fis = null;
		client.connect(AppConfig.getProperty("Custodian.Directory.Path"));
		client.login(AppConfig.getProperty("Custodian.FTP.Username"), AppConfig.getProperty("Custodian.FTP.Password"));////

		String filename = Dir+fname;	
		client.setFileType(FTP.BINARY_FILE_TYPE, FTP.BINARY_FILE_TYPE);
		client.setFileTransferMode(FTP.BINARY_FILE_TYPE);

		try{
			fis = new FileInputStream(filename);		
			client.storeFile(fname, fis);    
			fis.close();
			client.logout();
		}catch(Exception ex){
			//ex.printStackTrace();
			System.out.println("FTP");
		}
		System.out.println("your file with id "+fname+"successfully uploaded to server");


		return flag;

	}
	/*private boolean dtnFileRead(String filePathStr)
	{
		System.out.println("Start Dtnreader");
		boolean flag = false ;
		File fileRead = new File(filePathStr) ;
		if(fileRead.exists())
		{
			//ObjectInputStream objectIn = null ;
			//FileInputStream objectIn = null ;
			FileInputStream fin = null;
			ObjectInputStream ois = null;
			try{
				//objectIn = new ObjectInputStream(new FileInputStream(filePathStr)) ;
				//objectIn = new FileInputStream(filePathStr) ;

				fin = new FileInputStream(filePathStr);					
				ois = new ObjectInputStream(fin);//error here					

				while(true)
				{

					Packet packet = (Packet) ois.readObject();
					//Packet packet = (Packet) objectIn.readObject();
					mpContent = StateManager.getDownMap();
					String data = packet.getName();
					int offset = packet.getSequenceNumber();
					byte[] segment = packet.getData();
					store.write(data, offset, segment);

					ContentState conProp = mpContent.get(data);
					BitSet bs = conProp.bitMap;
					if(stateManager != null)
					{

						try	
						{
							int currentsegments = conProp.currentSegments;
							if(currentsegments == conProp.getTotalSegments())
							{

							}
							else
							{
								if(bs.get((int) (offset/segmentSize))==false)
								{

									currentsegments++;
									bs.set((int) (offset/segmentSize));
								}
								conProp.currentSegments = currentsegments;
								conProp.bitMap = bs ;
								mpContent.put(data,conProp);

								if(currentsegments == conProp.getTotalSegments())
								{
									System.out.println("In Reassembler.java Received the Complete File! :D :D :D :D :D :D :D :D :D :D :D :D ");
									Status st = Status.getStatus();
									String fileType = st.getContentType("uploadrequest",data);
									if(fileDownloads != null)
									{

										if(fileType.length()!=0){

											fileDownloads.put(data+"."+fileType);
										}												
										else{

											fileDownloads.put(data);
										}
									}	
									if(fileType.length()!=0){

										st.insertData("localdata", data+"."+fileType);
									}
									else{

										st.insertData("localdata", data);
									}

									st.updateState("status",data, 0); 
									//objectIn.close();
									ois.close();
									boolean del = fileRead.delete();
									System.out.println("file is deleted: "+del);
									System.out.println("Now you can remove your drive");	

									//code for system TrayIcon

									Toolkit toolkit = Toolkit.getDefaultToolkit();
									Image image = toolkit.getImage("E:/logo_usb.jpg");
									TrayIcon trayIcon = new TrayIcon(image, "TrayIcon");

									SystemTray tray = SystemTray.getSystemTray();
									try
									{
										tray.add(trayIcon);
									}
									catch (Exception e)
									{
										System.err.println("TrayIcon could not be added.");							           
									}

									trayIcon.displayMessage("Remove USB", "Please Safely remove your usb Drive", TrayIcon.MessageType.INFO);

									//code end for safely remove drive
								}
							}
						}
						catch(Exception e)
						{ 
							//e.printStackTrace();
							System.out.println("Exception in dtnFileRead DTNReader");
						}
					}

				}

			}
			catch(EOFException e)
			{
				//e.printStackTrace();
			}
			catch(Exception e)
			{	
				//e.printStackTrace();
				System.out.println("Exception in DTNReader in dtnFile Read just before finally");
			}
			finally
			{	try
			{	if(ois != null)
			{	ois.close();
			flag = true ;
			}	
			}
			catch(Exception e)
			{	System.out.println("Exception in DTNReader.java finally block : objectIn not found");
			}
			}
		}
		return flag;
	}*/

	public boolean ftpclient(String filename) throws IOException, IOException{

		Socket soc=new Socket("10.22.6.90",5217);
		boolean flag = false;
		Socket ClientSoc;

		DataInputStream din = null;
		DataOutputStream dout = null;
		BufferedReader br = null;		

		try
		{
			ClientSoc=soc;
			din=new DataInputStream(ClientSoc.getInputStream());
			dout=new DataOutputStream(ClientSoc.getOutputStream());
			br=new BufferedReader(new InputStreamReader(System.in));
		}
		catch(Exception ex)
		{
		}

		dout.writeUTF("SEND");		

		File f=new File(filename);
		if(!f.exists())
		{
			System.out.println("File not Exists...");
			dout.writeUTF("File not found");
			return flag;
		}

		dout.writeUTF(filename);

		String msgFromServer=din.readUTF();
		if(msgFromServer.compareTo("File Already Exists")==0)
		{
			String Option;
			System.out.println("File Already Exists. Want to OverWrite (Y/N) ?");
			Option=br.readLine();			
			if(Option=="Y")	
			{
				dout.writeUTF("Y");
			}
			else
			{
				dout.writeUTF("N");
				return flag;
			}
		}

		System.out.println("Sending File ...");
		FileInputStream fin=new FileInputStream(f);
		int ch;
		do
		{
			ch=fin.read();
			dout.writeUTF(String.valueOf(ch));
		}
		while(ch!=-1);
		fin.close();
		System.out.println(din.readUTF());

		return false;

	}

}

/*
	private boolean dtnFileRead(String filePathStr)
	{
		boolean flag = false ;
		File fileRead = new File(filePathStr) ;
		if(fileRead.exists())
		{
			FileInputStream fis = null ;
			try{
					fis = new FileInputStream(filePathStr);
					while(true)
					{
						byte[] dataIn = null;
						int check = fis.read(dataIn);
						//Packet packet = (Packet) objectIn.readObject();
						mpContent = StateManager.getDownMap();
						//String data = packet.getName();
						//int offset = packet.getSequenceNumber();
						//byte[] segment = packet.getData();
						//store.write(data, offset, segment);
						store.write(dataname, data)
						//ContentState conProp = mpContent.get(data);
						//BitSet bs = conProp.bitMap;
						if(stateManager != null)
						{
							try	
							{
								int currentsegments = conProp.currentSegments;
								if(currentsegments == conProp.getTotalSegments())
								{

								}
								else
								{
									if(bs.get((int) (offset/segmentSize))==false)
									{
										currentsegments++;
										bs.set((int) (offset/segmentSize));
									}
									conProp.currentSegments = currentsegments;
									conProp.bitMap = bs ;
									mpContent.put(data,conProp);
									if(currentsegments == conProp.getTotalSegments())
									{
										System.out.println("In Reassembler.java Received the Complete File! :D :D :D :D :D :D :D :D :D :D :D :D ");
										Status st = Status.getStatus();
										String fileType = st.getContentType("uploadrequest",data);
										if(fileDownloads != null)
										{	
											if(fileType.length()!=0)
												fileDownloads.put(data+"."+fileType);
											else
												fileDownloads.put(data);
										}	
										if(fileType.length()!=0)
											st.insertData("localdata", data+"."+fileType);
										else
											st.insertData("localdata", data);
										st.updateState("status",data, 0); 
										objectIn.close();
										boolean del = fileRead.delete();
										System.out.println("file is deleted: "+del);
										System.out.println("Now you can remove your drive");	
										JFrame parent = new JFrame();
										JOptionPane.showMessageDialog(parent, "Safely Remove Drive");
									}
							}
						}
						catch(Exception e)
						{ 
							System.out.println("Exception in dtnFileRead DTNReader");
						}
					}

				}

			}
			catch(EOFException e)
			{
			}
			catch(Exception e)
			{	System.out.println("Exception in DTNReader in dtnFile Read just before finally");
			}
			finally
			{	try
				{	if(objectIn != null)
					{	objectIn.close();
						flag = true ;
					}	
				}
				catch(Exception e)
				{	System.out.println("Exception in DTNReader.java finally block : objectIn not found");
				}
			}
		}
		return flag;
	}
}
 */