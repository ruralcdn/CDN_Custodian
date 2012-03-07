package AbstractAppConfig;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.Properties;

public class AppConfig{
	
	private static Properties config;
	
	public AppConfig()
	{
		config = new Properties();
	}
	public AppConfig(Properties defaults)
	{
		config = new Properties(defaults);
	}
	public static Properties getConfig()
	{
		return config;
	}
	public static void load(InputStream in) throws IOException
	{
	config.load(in);	
	}
	public static String getProperty(String key)
	{
		return config.getProperty(key);
	}
	public static void setProperty(String key,String value)
	{
		config.setProperty(key, value);
	}
	
	
	@SuppressWarnings("rawtypes")
	public static Enumeration propertyNames() 
	{
		return config.propertyNames();
	}
	public static void store(OutputStream out, String header) throws IOException
	{
		config.store(out,header);
	}
}