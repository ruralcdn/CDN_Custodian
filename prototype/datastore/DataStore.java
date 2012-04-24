package prototype.datastore;

import java.io.*;



public class DataStore{
		
	String dirPath;
	public int var;
	public DataStore(String path) 
	{
		dirPath = path;    			
	}
	public String getPath()
	{
		return dirPath;
	}
	public boolean contains(String dataname)
	{
		File file = new File(dirPath+dataname);
		return file.exists();
	}
	/*public boolean contains (String dataname){
		System.out.println("dtaname"+dataname);
		boolean exists = false;
		Status st = Status.getStatus();
		exists = st.containsData("localdata", dataname);
		return exists ;
	}*/
	public File getFile(String dataname)
	{
		return (new File(dirPath+dataname));
	}
	public byte[] read(String dataname)//synchronized 
	{
		
	        //System.out.println("File name: "+ dataname);    
	        File file = new File(dirPath+dataname); 
	        //System.out.println("reading file"+file.getName());
	        //File length
	        int size = (int)file.length(); 
	        if (size > Integer.MAX_VALUE){
	          System.out.println("File is to larger");
	        }
	        byte[] bytes = new byte[size]; 
	        try{
	        DataInputStream dis = new DataInputStream(new FileInputStream(file)); 
	        int read = 0;
	        int numRead = 0;
	        while (read < bytes.length && (numRead=dis.read(bytes, read,
	                                                  bytes.length-read)) >= 0) {
	          read = read + numRead;
	        }
	       // System.out.println("File size: " + read);
	        // Ensure all the bytes have been read in
	        if (read < bytes.length) {
	          System.out.println("Could not completely read: "+file.getName());
	        }
	        dis.close();
	      }
	      catch (Exception e){
	        e.getMessage();
	      }
	return bytes;	
	}
	
	public byte[] read(String dataname,int offset,int length)  //synchronized
	{
		
			
	        //System.out.println("File name: "+ dataname);    
	        File file = new File(dirPath+dataname); 
	      //  System.out.println("reading file "+file.getName()+ " thru method read in dataStore.java");
//	        System.out.println("offset inside store = "+offset);
 

	        byte[] bytes = new byte[length]; 
	      //  System.out.println("bytes length: "+bytes.length);
	        try{
	        DataInputStream dis = new DataInputStream(new FileInputStream(file)); 
	        dis.skipBytes(offset);
	        int read = 0;
	        int numRead = 0;

	        while (read < bytes.length && (numRead=dis.read(bytes,read,bytes.length-read)) >= 0) {
	        	//System.out.println("NumRead: "+numRead);
	        	//System.out.println("bytes read "+ new String(bytes));
	          read = read + numRead;
	        }
	//        System.out.println("File size: " + read);
	        
	        // Ensure all the bytes have been read in
	        if (read < bytes.length) {
	          System.out.println("Could not completely read: "+file.getName()+" thru method read datastore.java");
	        }
	        dis.close();
	      }
	      catch (Exception e){
	        e.printStackTrace();
	      }
	return bytes;	
	}
	
	public long length(String dataname)
	{
		 File file = new File(dirPath+dataname); 
		 return file.length();
	}
	public synchronized boolean write(String dataname,byte[] data) 
	{
		
		System.out.println("File name: in write method "+dataname+" path= "+dirPath+" thru method write datastore.java");
		File file = new File(dirPath+dataname);
		File newFile= new File(dirPath+dataname+".zip");
		try
		{
		if(file.exists())	
			file.renameTo(newFile);
			
		/*FileOutputStream fos = new FileOutputStream(file);
		fos.write(data);
		fos.close();*/
		}
		catch(Exception ex)
		{
		//System.out.println("FileNotFoundException : " + ex);
		}
		
	return true;	
	}
	public synchronized boolean write(String dataname,long offset,byte[] data)
	{
		File file = new File(dirPath+dataname);
		//System.out.println("filename write"+file);
		RandomAccessFile accessFile;
		try {
			accessFile = new RandomAccessFile(file,"rw");
			accessFile.seek(offset);
			accessFile.write(data);
			accessFile.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		} 
		return true;
	}
	public synchronized boolean rename(String dataname, String fileType)
	{
		boolean flag = false ;
		File oldFile = new File(dirPath+dataname);
		File newFile = new File(dirPath+dataname+"."+fileType);
		if(oldFile.exists()){
			oldFile.renameTo(newFile);
			flag = true;
		}	
		return flag ;
	}
	public synchronized boolean append(String dataname,byte[] data) 
	{
		
		System.out.println("File name: in write method "+dataname+" path= "+dirPath+" thru method append datastore.java");
		File file = new File(dirPath+dataname);
		try
		{

		FileOutputStream fos = new FileOutputStream(file,true);
		fos.write(data);
		fos.close();
		}
		catch(FileNotFoundException ex)
		{
		//System.out.println("FileNotFoundException : " + ex);
		}
		catch(IOException ioe)
		{
		//System.out.println("IOException : " + ioe);
		}
	return true;	
	}
	
	public boolean delete(String dataname) 
	{
		
		File file = new File(dirPath+dataname);		
		return file.delete();
	}

}
