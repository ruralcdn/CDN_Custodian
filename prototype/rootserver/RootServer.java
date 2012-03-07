package prototype.rootserver;

import java.io.File;
import java.io.FileInputStream;
import java.rmi.registry.Registry;

import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector; 
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.Semaphore;

import AbstractAppConfig.AppConfig;
	
public class RootServer implements IRootServer {
		
    private Map<String,List<String>> dataCacheLocMap;
    private Semaphore lookupMutex;
    
    
	public RootServer(Map<String,List<String>> m1,Semaphore mutex) {
		dataCacheLocMap = m1;	
		lookupMutex = mutex;
		List<String> value = new ArrayList<String>();
		String serviceInstanceInfo = AppConfig.getProperty("RootServer.ServiceInstance");
		value.add(serviceInstanceInfo);
		String serviceInstanceInfoKey = AppConfig.getProperty("RootServer.Name")+"$serviceInstance";
		dataCacheLocMap.put(serviceInstanceInfoKey, value);
	}

    public boolean register(String dataname,String source) {
    	
    	System.out.println("Registering :"+dataname);
    	synchronized(dataCacheLocMap)
    	{
    	if(dataCacheLocMap.containsKey(dataname))
    	{
    		List<String> val = dataCacheLocMap.get(dataname);
    		if(!val.contains(source))
    		val.add(0,source);
    		dataCacheLocMap.put(dataname, val);
    			
    	}
    	else
    	{
    	List<String> val = new Vector<String>();
    	val.add(0,source);
    	dataCacheLocMap.put(dataname, val);	
    	}	
    	}
    	return true;
    }
    
    public boolean deregister(String dataname,String source) {
    	
    	System.out.println("Deregistering :"+dataname);
    	synchronized(dataCacheLocMap)
    	{
    	if(dataCacheLocMap.containsKey(dataname))
    	{
    		List<String> val = dataCacheLocMap.get(dataname);
    		val.remove(source);
    		dataCacheLocMap.put(dataname, val);
    		
    	}
    	lookupMutex.release();
    	
    	}
    	return true;
        }
    
    public List<String> find(String dataname) {
    	return dataCacheLocMap.get(dataname);
    }
	
    public static void main(String args[]) {
	
	try {
		
		File configFile = new File("config/RootServer.cfg");
		FileInputStream fis;
		fis = new FileInputStream(configFile);
		new AppConfig();
		AppConfig.load(fis);
		fis.close();
		
		
		Map<String,List<String>> map = new HashMap<String,List<String>>();
		Semaphore mutex = new Semaphore(1,true);
		RootServer obj = new RootServer(map,mutex);
	    IRootServer stub = (IRootServer) UnicastRemoteObject.exportObject(obj, 0);
	    

	    // Bind the remote object's stub in the registry
	    Registry registry = LocateRegistry.getRegistry();
	    registry.bind(AppConfig.getProperty("RootServer.Service"), stub);
	    System.out.println("Service name: "+AppConfig.getProperty("RootServer.Service"));
	    System.err.println("Server ready");
	} catch (Exception e) {
	    System.err.println("Server exception: " + e.toString());
	    e.printStackTrace();
	}
    }
}
