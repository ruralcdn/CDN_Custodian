package NewStack;

import java.util.HashMap;
import java.util.Map;

import newNetwork.Connection;

public class PolicyModule{
	
	/*
	 * stores a Map of AppId/dataRequesterId to the 
	 * ConnectionType over which they want to get the data
	 * AppId will be used by user 
	 * DataRequesterId will be used by caches/custodians
	 */
	private Map<String,Connection.Type> policyMap;
	private Connection.Type defaultType = Connection.Type.DSL; //config driven
	public PolicyModule()
	{
		policyMap = new HashMap<String,Connection.Type>();
	}
	
	public void setPolicy(String appId,Connection.Type type)
	{
		policyMap.put(appId,type);
	}
	
	public Connection.Type getConnectionType(String Id)
	{
		Connection.Type type = policyMap.get(Id);
		if(type == null)
			return defaultType;
		else
			return type;
	}
	
}