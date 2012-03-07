package prototype.rendezvous;

import java.io.File;
import java.io.FileInputStream;
import java.rmi.registry.Registry;


import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.NotBoundException;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.StringTokenizer;

import AbstractAppConfig.AppConfig;

import prototype.rootserver.IRootServer;

public class RendezvousServer implements IRendezvous{

	Map<String,List<String>> localContentLookup; 

	public RendezvousServer(Map<String,List<String>> contentLookup) 
	{
		localContentLookup  = contentLookup;
	}

	public List<String> find(String dataname) throws RemoteException,NotBoundException
	{
		if(localContentLookup.containsKey(dataname))
			return localContentLookup.get(dataname);
		else
		{
			String rootServer = null;
			//parser for dataname to get servername and may have to create DNS lookup
			StringTokenizer st = new StringTokenizer(dataname,"$");
			if(st.hasMoreElements())
				rootServer = st.nextToken();		
			System.out.println("Contacting Root Server");
			Registry registry;		
			IRootServer stub;
			try
			{
				registry = LocateRegistry.getRegistry(rootServer);
				stub = (IRootServer) registry.lookup(AppConfig.getProperty("Rendezvous.RootServer.Service"));   //create a parser and make it config driven
			}catch(RemoteException e)
			{
				registry = LocateRegistry.getRegistry(AppConfig.getProperty("Rendezvous.DefaultRootServer").trim());
				stub = 	(IRootServer) registry.lookup(AppConfig.getProperty("Rendezvous.RootServer.Service")); 
			}
			List<String> l = stub.find(dataname);
			System.out.println("response in rendezvous: "+l);
			synchronized(localContentLookup)
			{
				localContentLookup.put(dataname,l);
			}
			return l;
		}
	}

	public static void main(String args[]) {	
		try {

			File configFile = new File("config/Rendezvous.cfg");
			FileInputStream fis;
			fis = new FileInputStream(configFile);
			new AppConfig();
			AppConfig.load(fis);
			fis.close();

			Map<String,List<String>> lookup = new HashMap<String,List<String>>();
			RendezvousServer obj = new RendezvousServer(lookup);
			IRendezvous stub = (IRendezvous) UnicastRemoteObject.exportObject(obj, 0);

			// Bind the remote object's stub in the registry
			Registry registry = LocateRegistry.getRegistry();
			registry.bind(AppConfig.getProperty("Rendezvous.Service") , stub);

			System.err.println("Server ready");

		} catch (Exception e) {
			System.err.println("Server exception: " + e.toString());
			e.printStackTrace();
		}
	}
}
