package prototype.userregistrar;


import java.io.File;
import java.io.FileInputStream;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import AbstractAppConfig.AppConfig;
import prototype.utils.AlreadyRegisteredException;
import prototype.utils.AuthenticationFailedException;
import prototype.utils.NotRegisteredException;


public class CustodianLogger implements ICustodianLogin {	

	Map<String,String> users;
	Map<String,List<String>> userCustodianLookup;
	Map<String,String> activeCustodianLookup;


	public CustodianLogger(Map<String,String> userPasswords,Map<String,List<String>> usercustodianlookup,Map<String,String> activecustodianlookup)
	{

		users = userPasswords;
		userCustodianLookup = usercustodianlookup;
		activeCustodianLookup = activecustodianlookup;
	}

	public String get_active_custodian(String userId) throws RemoteException
	{
		return activeCustodianLookup.get(userId);
	}

	public boolean register_user(String userId,String password) throws RemoteException,AlreadyRegisteredException
	{
		System.out.println("Inside CustodianLogger.java: register_user");
		if(users.get(userId) == null)
		{
			users.put(userId,password);
			List<String> custodians = new ArrayList<String>();
			synchronized(userCustodianLookup)
			{
				userCustodianLookup.put(userId, custodians);
			}
			synchronized(activeCustodianLookup)
			{
				activeCustodianLookup.put(userId,"");
			}
			return true;
		}
		else
			throw new AlreadyRegisteredException();

	}

	public IUserRegistrarSession authenticate_user(String userId,String password,String custodianId) throws RemoteException,AuthenticationFailedException,NotRegisteredException
	{
		System.out.println("Inside CustodianLogger.java: authenticate_user");
		System.out.println("CustodianId: " + custodianId);
		if(users.get(userId).equals(password) && userCustodianLookup.get(userId).contains(custodianId))
		{
			//check if there's an already active custodian,then disconnect the current one and connect from the new one
			synchronized(activeCustodianLookup)
			{
				activeCustodianLookup.put(userId, custodianId);
			}
			IUserRegistrarSession Session = new RegistrarSession(userId,custodianId,userCustodianLookup,activeCustodianLookup);
			IUserRegistrarSession stub = (IUserRegistrarSession) UnicastRemoteObject.exportObject(Session, 0);
			return stub;
		}
		else
			throw new AuthenticationFailedException();

	}

	public boolean register_user_custodian(String userId,String custodianId) throws RemoteException,NotRegisteredException
	{
		synchronized(userCustodianLookup)
		{
			List<String> custodians = userCustodianLookup.get(userId);
			custodians.add(custodianId);
			userCustodianLookup.put(userId, custodians);
			return true;
		}
	}
	public static void main(String args[]) {

		try {

			File configFile = new File("config/UserRegistrar.cfg");
			System.out.println("Absolute path of the file:"+configFile.getAbsolutePath());
			FileInputStream fis;
			fis = new FileInputStream(configFile);
			new AppConfig();
			AppConfig.load(fis);
			fis.close();

			Map<String,String> users = new HashMap<String,String>();
			Map<String,List<String>> userCustodianLookup = new HashMap<String,List<String>>();
			Map<String,String> activeCustodianLookup = new HashMap<String,String>();

			CustodianLogger obj = new CustodianLogger(users,userCustodianLookup,activeCustodianLookup);

			ICustodianLogin stub = (ICustodianLogin) UnicastRemoteObject.exportObject(obj, 0);

			// Bind the remote object's stub in the registry
			Registry registry = LocateRegistry.getRegistry();
			System.out.println("Service name: "+AppConfig.getProperty("UserRegistrar.Service"));
			registry.bind(AppConfig.getProperty("UserRegistrar.Service") , stub);

			System.err.println("Server ready");
			System.out.println("Here I am the Logger ..yes THE LOGGER");
		} catch (Exception e) {
			System.err.println("Server exception: " + e.toString());
			e.printStackTrace();
		}

	}

	@Override
	public boolean new_registration(Map<String, String> userInfo)
			throws RemoteException {
		// TODO Auto-generated method stub
		return false;
	}
}





