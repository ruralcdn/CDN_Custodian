package prototype.utils;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface IUserRegistrar extends Remote {	
	
	public boolean register_user_custodian(String userId,String custodianId) throws RemoteException;
	public boolean unregister_user_custodian(String userId,String custodianId) throws RemoteException;
	public boolean register_user(String userId,String password) throws RemoteException;
	public boolean register_custodian(String custodianId,String password) throws RemoteException;
	public boolean authenticate_user(String userId,String password) throws RemoteException;
	public boolean authenticate_custodian(String custodianId,String password) throws RemoteException;
	public int notify_connection(String userId,String custodianId) throws RemoteException;
	public boolean close_connection(String userId,String custodianId) throws RemoteException;
	public List<String> get_custodians(String userId) throws RemoteException;
	
}