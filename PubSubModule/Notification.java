package PubSubModule;

import java.io.Serializable;

public class Notification implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public enum Type{UploadAck,Publish}
	private String notificationContent;
	private Type notificationType;
	public Notification(Type type,String content)
	{
		notificationType = type;
		notificationContent = content;
	}
	public String getContent()
	{
		return notificationContent;
	}
	public Type getNotificationType()
	{
		return notificationType;
	}
}