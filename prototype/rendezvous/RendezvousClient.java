package prototype.rendezvous;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;

public class RendezvousClient {

    private RendezvousClient() {}

    public static void main(String[] args) {

	String host = (args.length < 1) ? null : args[0];
	try {
	    Registry registry = LocateRegistry.getRegistry(host);
	    IRendezvous stub = (IRendezvous) registry.lookup("rendezvous");
	    List<String> l = stub.find("xyz.txt");
	    System.out.println("response: "+l);

	} catch (Exception e) {
	    System.err.println("Client exception: " + e.toString());
	    e.printStackTrace();
	}
    }
}
