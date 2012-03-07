package prototype.userregistrar;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;

import prototype.utils.AlreadyRegisteredException;
import prototype.utils.AuthenticationFailedException;
import prototype.utils.NotRegisteredException;

public interface ICustodianLogin extends Remote {	
	
	public String get_active_custodian(String userId) throws RemoteException;
	public boolean register_user(String userId,String password) throws RemoteException,AlreadyRegisteredException;
	public IUserRegistrarSession authenticate_user(String userId,String password,String custodianId) throws RemoteException,AuthenticationFailedException,NotRegisteredException;
	public boolean register_user_custodian(String userId,String custodianId) throws RemoteException,NotRegisteredException;
	public boolean new_registration(Map<String,String> userInfo) throws RemoteException ;
}