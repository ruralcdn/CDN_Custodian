package DBSync;

@SuppressWarnings("serial")
public class Tuple implements java.io.Serializable {
	int id ;
	String key ;
	String value ;
	public Tuple(int i, String k, String v)
	{
		id = i;
		key = k ;
		value = v ;
	}
	public String getKey(){
		return key ;
	}
	public String getValue(){
		return value ;
	}
	public int getid(){
		return id ;
	}
}
