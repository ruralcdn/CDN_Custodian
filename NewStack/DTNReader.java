package NewStack;

import java.io.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import StateManagement.ContentState;
import StateManagement.StateManager;
import StateManagement.Status;
import prototype.datastore.DataStore;
import javax.swing.* ;
public class DTNReader extends Thread {
	String osName ;
	String dirPathinLin ;
	List <String> filesinLin ;
	List<String> readingList ;
	DataStore store ;
	StateManager stateManager;
	int segmentSize;
	private BlockingQueue<String> fileDownloads;
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
		fileDownloads = downloads ;
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
				Thread.sleep(2000);
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
								String[] dataList = dtnDir.list(); 
								for(int j = 0; j < dataList.length ; j++)
								{
									if(readingList.contains(dataList[j])){
										//String fileName = readingList.remove(0) ;
										String fileName = dataList[j];
										filePathStr = filePathStr+fileName;
										System.out.println("File Name is: "+filePathStr);
										if(dtnFileRead(filePathStr)){
											System.out.println("File is successfully read");
											readingList.remove(dataList[j]);
										}	
										else{
											try {
												Thread.sleep(2000);
											} catch (Exception e) {
												e.printStackTrace();
											}
										}	
										System.out.println("Reading List size is: "+readingList.size());
									}
								}
								System.out.println("Now you can remove your drive");	
								JOptionPane.showMessageDialog(null, "Safely Remove USBdrive");
							} 
							catch (IOException e) 
							{
								System.out.println("Error in reading or writing file");
								try {
									Thread.sleep(5000);
								} catch (InterruptedException e1) {
									e1.printStackTrace();
								}
							}
						}
						else
						{
							System.out.println("Drive "+letters[i]+" has been unplugged");
						}
						isDrive[i] = pluggedIn;
					}
				}
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
								System.out.println("Reading List size is: "+readingList.size());
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


	private boolean dtnFileRead(String filePathStr){
		boolean flag = false ;
		File fileRead = new File(filePathStr) ;
		if(fileRead.exists()){
			ObjectInputStream objectIn = null ;
			try{
				objectIn = new ObjectInputStream(new FileInputStream(filePathStr)) ;
				while(true){

					Packet packet = (Packet) objectIn.readObject();
					
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
							if(currentsegments == conProp.getTotalSegments()){

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
									{	if(fileType.length()!=0)
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
								}
							}
						}catch(Exception e){ 
							System.out.println("Exception in dtnFileRead DTNReader");
						}
					}

				}

			}catch(EOFException e){

			}
			catch(Exception e){				
				//e.printStackTrace();
				System.out.println("Exception in DTNReader in dtnFile Read just before finally");
			}
			finally{
				try{
					if(objectIn != null){
						objectIn.close();
						flag = true ;
					}	
				}catch(Exception e){
					System.out.println("Exception in DTNReader.java finally block : objectIn not found");

				}
			}
		}
		return flag;
	}
}
