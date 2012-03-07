package prototype.rootserver;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;

public class RootClient {

    private RootClient() {}

    public static void main(String[] args) {

	String host = (args.length < 1) ? null : args[0];
	try {
	    Registry registry = LocateRegistry.getRegistry(host);
	    IRootServer stub = (IRootServer) registry.lookup("rootserver");
	    
	    stub.register("abc","xyz");
	    stub.register("abc","pqr");
	    stub.register("abc","def");
	    stub.register("xyz","def");
	    stub.register("xyz","abc");
	    stub.register("xyz","def");
	    List<String> l = stub.find("xyz");
	    System.out.println("response: "+l);

	    stub.deregister("xyz", "def");
	    stub.deregister("xyz", "abc");
	    
	    l = stub.find("xyz");
	    System.out.println("response: "+l);
	    
	    //System.out.println("response: " + response);
	} catch (Exception e) {
	    System.err.println("Client exception: " + e.toString());
	    e.printStackTrace();
	}
    }
}
