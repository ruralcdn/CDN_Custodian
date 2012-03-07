package prototype.utils;

public class AnyClient{
	
	protected String clientId;
//	protected String password;
	
	public AnyClient(String Id)
	{
		clientId = new String(Id);
		//password = new String(passwd);
	}
	public String getId()
	{
		return clientId;
	}
}

class User extends AnyClient{
	
	public User(String userId){
		super(userId);
	}
}

class Custodian extends AnyClient{
	
	public Custodian(String custodianId){
		super(custodianId);
	}
}

