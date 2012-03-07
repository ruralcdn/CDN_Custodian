package NewStack;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.swing.JOptionPane;

import NewStack.Packet;
import StateManagement.ContentState;
import StateManagement.StateManager;
import prototype.datastore.DataStore;
import newNetwork.USBConnection;
import newNetwork.Connection;
import newNetwork.TCPConnection;

public class LinkDetector extends Thread{

	private String connectionId;
	private static List<String> destinationConnectionIds;
	private List<String> localIPs;
	private Scheduler scheduler;
	private static boolean flag = true ;
	private static int dataConnectionId;
	private List<Integer> connectionPorts;	
	Map<String, ContentState> mpUp ;

	/**
	 * Newly added parameters for DTN Transfer
	 * @letters: for each drive
	 * @drives: for treating each drive as file
	 * @isDrive: for checking whether drives attached or not
	 */
	String[] letters = new String[]{ "A", "B", "C", "D", "E", "F", "G", "H", "I", 
			"J", "K", "L", "M", "N", "O", "P", "Q", "R",
			"S", "T", "U", "V", "W", "X", "Y", "Z" };
	File[] drives = new File[letters.length];
	boolean[] isDrive = new boolean[letters.length];
	private static List<String> dtnDestinationIds;

	String osName ;
	String dirPathinLin ;
	List <String> filesinLin ;

	public LinkDetector(String Id,Scheduler sched,List<Integer> portList,DataStore dStore,DataStore usbStore)
	{
		osName= System.getProperty("os.name");
		connectionId = Id;
		scheduler = sched;
		localIPs = new ArrayList<String>();
		dataConnectionId = 0;
		connectionPorts = portList;
		destinationConnectionIds = new ArrayList<String>();
		mpUp = new HashMap<String, ContentState>();
		dtnDestinationIds = new ArrayList<String>();
		for(int i = 0 ; i < letters.length ; i++){
			drives[i] = new File(letters[i]+":/");
			isDrive[i] = drives[i].canRead();
		}
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

	public boolean addDestination(String destination){
		try 
		{

			Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
			for(NetworkInterface netInf : Collections.list(nets))
			{
				if(!netInf.isPointToPoint() && !netInf.isLoopback())
				{
					Enumeration<InetAddress> inetAdd = netInf.getInetAddresses();
					for(InetAddress inet : Collections.list(inetAdd))
					{
						if(!localIPs.contains(inet.getHostAddress()))
						{
							localIPs.add(inet.getHostAddress());
						}
					}
				}
			}
		} 
		catch (SocketException e) 
		{
			e.printStackTrace();
		}

		String[] connectionInfo = destination.split(":");
		InetAddress local;
		boolean addDest = true ;
		try
		{
			InetAddress add = InetAddress.getByName(connectionInfo[0]);
			int port = Integer.parseInt(connectionInfo[1]);
			if(!destinationConnectionIds.contains(destination))
			{
				System.out.println("Adding the Destinations in Scheduler");
				for(int i = 0;i < localIPs.size();i++)
				{
					local = InetAddress.getByName(localIPs.get(i));
					if(!connectionPorts.isEmpty())
					{
						dataConnectionId++;
						Connection con;
						try 
						{
							con = new TCPConnection(dataConnectionId,add,port,local,connectionPorts.get(0));
							connectionPorts.remove(0);
							System.out.println("New Connection created thru method addDes in Link Detct elseif");
							Packet packet = new Packet(connectionId); // authentication packet 
							con.writePacket(packet);
							flag = false ;
							System.out.println("NEW Connection Established in link detectorthru method addDes in Link Detct elseif");
							scheduler.addConnection(connectionInfo[0],con);
							//if(!destinationConnectionIds.contains(destination))
							destinationConnectionIds.add(destination);


						}
						catch (ConnectException e)
						{
							System.out.println("Serever is terminated or Network unreachable");
							mpUp = StateManager.getUpMap();
							Set<String> key = mpUp.keySet();
							Iterator<String> it = key.iterator();
							while(it.hasNext())
							{
								String contentId = it.next();
								ContentState contentState = mpUp.get(contentId);
								if(contentState.getPreferredRoute().contains(destination))
								{	
									contentState.currentSegments = 0 ;
									mpUp.put(contentId,contentState);
									destinationConnectionIds.remove(destination);
								}	

							}
							addDest = false ;

						}
						catch (IOException e)
						{
							System.out.println("Link Failure occured");
							if(!flag)
							{
								mpUp = StateManager.getUpMap();
								Set<String> key = mpUp.keySet();
								Iterator<String> it = key.iterator();
								while(it.hasNext())
								{
									String contentId = it.next();
									ContentState contentState = mpUp.get(contentId);
									/**
									 * Here below the value 50 should be configuration derived
									 * Right now we have 5 Queues and each queue contains 10 packets
									 * So that total loss at max is of 50 packets in case of Link failure 
									 */
									if(contentState.currentSegments > 50)
										contentState.currentSegments -= 50 ;
									else
										contentState.currentSegments = 0 ;
									mpUp.put(contentId,contentState);
									//destinationConnectionIds.remove(destination);

								}
								flag = true ;
							}
							addDest = false ;
						}
					}	
				}	
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return addDest ;
	}


	public void close()
	{
	}

	public boolean addDTNDestination(String destination){
		boolean checkUSB = true ;
		String[] connectionInfo = destination.split(":");

		if(!dtnDestinationIds.contains(destination)){

			String dtnDir = findUSB(connectionInfo[0]);
			if(dtnDir != null){
				USBConnection con = new USBConnection(dtnDir);
				scheduler.addConnection(connectionInfo[0], con);
				dtnDestinationIds.add(destination);
			}
			else
				checkUSB = false ;
		}
		return checkUSB ;
	}

	public void removeDTNdestination(String destination)
	{
		String[] connectionInfo = destination.split(":");
		dtnDestinationIds.remove(destination);
		scheduler.removeDTNConnection(connectionInfo[0]);
		System.out.println("Safely remove the drive");
		JOptionPane.showMessageDialog(null,"Safely remove the drive");
	}
	public static void setDestinationIds(List<String> destinationIds)
	{
		destinationConnectionIds = destinationIds ;
		System.out.println("destinationConnectionIds in Link Detector: " + destinationConnectionIds);

	}

	public static List<String> getDestinationIds()
	{
		return destinationConnectionIds ;

	}

	public String findUSB(String dest){
		String dtnDir = null ;
		if(osName.contains("Windows")){
			for (int i = 0; i < letters.length; ++i)
			{
				boolean pluggedIn = drives[i].canRead();
				if (pluggedIn != isDrive[i])
				{
					if(pluggedIn)
					{	                
						System.out.println("Drive "+letters[i]+" has been plugged in");
						String str = "cmd /c \"dir "+ letters[i] + ":\"";
						boolean execute = true;
						while(execute)
						{
							try 
							{
								Process process =Runtime.getRuntime().exec(str);
								BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
								boolean file = false;

								if(findInUSB(input,file,"DTNRouter"))
								{
									dtnDir = letters[i] + ":DTNRouter\\";
									String usbIdFile = dtnDir+"USB.cfg";
									FileInputStream fis = new FileInputStream(usbIdFile);
									Properties prop = new Properties();
									prop.load(fis);
									String destId = prop.getProperty("USB.owner");
									System.out.println("Destination and User Daemon destination is: "+dest+", "+destId);
									if(!destId.equals(dest)){
										return null ;
									}	
									System.out.println("The USB key is a DTNRouter.\nPlease do not unplugged it until all request copied in the USB");
								}
								execute = false; // to terminate the loop
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
				//System.out.println("No device plugged in");
				
			}	
			else if(temp.size() > filesinLin.size()){
				for(int i = 0 ; i  < temp.size();i++){
					if(!filesinLin.contains(temp.get(i))){
						newDev = temp.get(i);
						break ;
					}	

				}
				filesinLin = temp ;
				System.out.println("Plugged Device in LinkDetector: "+newDev);
				dtnDir = dirPathinLin+newDev+"/";
				System.out.println("In LDetector, value of dtnDir: "+dtnDir);
				try{
					if(findInUSBinLin(dtnDir)){
						dtnDir += "DTNRouter/"; 
						String usbIdFile = dtnDir+"USB.cfg";
						FileInputStream fis = new FileInputStream(usbIdFile);
						Properties prop = new Properties();
						prop.load(fis);
						String destId = prop.getProperty("USB.owner");
						System.out.println("Destination and User Daemon destination is: "+dest+", "+destId);
						if(!destId.equals(dest)){
							return null ;
						}	
						System.out.println("The USB key is a DTNRouter.\nPlease do not unplugged it until all request copied in the USB");
					}
					else{
						System.out.println("The device is not  DTNRouter");
						
					}	
					
				}catch(Exception e){
					e.printStackTrace();
					System.out.println("Error in reading or writing file");
					try {
						Thread.sleep(5000);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
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
		return dtnDir ;
	}

	private  boolean findInUSBinLin(String newDevdir) 
	{
		try{
			File dir1 = new File(newDevdir);
			String[] files =dir1.list() ;
			for(int i = 0 ; i < files.length; i++){
				if(files[i].equals("DTNRouter"))
					return true ;
			}
		}catch(Exception e){
			e.printStackTrace();
			return false ;
		}
		return false ;  	
	}
	private boolean findInUSB(BufferedReader input,boolean file,String name) throws IOException
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

}