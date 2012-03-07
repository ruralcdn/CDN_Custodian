package prototype.userregistrar;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.Map;

public class RegistrarSession implements IUserRegistrarSession{

	String custodianId;
	String userId;
	Map<String,List<String>> userCustodianLookup;
	Map<String,String> activeCustodianLookup;

	public RegistrarSession(String userid,String custodianid,Map<String,List<String>> usercustodianlookup,Map<String,String> activecustodianlookup)
	{
		custodianId = custodianid;
		userId = userid;
		userCustodianLookup = usercustodianlookup;
		activeCustodianLookup = activecustodianlookup;
	}

	public boolean close_connection() throws RemoteException
	{
		synchronized(activeCustodianLookup)
		{
		activeCustodianLookup.put(userId,"");
		}
		return UnicastRemoteObject.unexportObject(this, true);
	}
	public List<String> get_custodians() throws RemoteException
	{
		return userCustodianLookup.get(userId);
	}

}