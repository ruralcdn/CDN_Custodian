package prototype.utils;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;

import java.util.concurrent.ArrayBlockingQueue;

public class FileReader extends Thread{
	
    File file;
    ArrayBlockingQueue<byte[]> buffer;
    boolean flag;
    
    public FileReader(File f,ArrayBlockingQueue<byte[]> b,boolean done){
    	this.file = f;
    	this.buffer = b;
    	this.flag = done;
    }
    
    public void run()
    {
    	try
    	{
    	DataInputStream dis = new DataInputStream(new FileInputStream(file));
    	byte[] buf = new byte[1024];
    	long size = file.length();
    	long read = 0;
    	int bufread = 0;
    	int nread = 0;
    	while(read < size)
    	{
    	while(bufread < buf.length && (nread = dis.read(buf,(int)read, buf.length - bufread)) >= 0)
    			{
    		 	 bufread = bufread+nread;
    			}
    	buffer.put(buf);
    	read = read+bufread;
    	}
    	}catch(Exception e)
    	{
    		System.out.println("Exception: "+e.getMessage());
    	}
    	flag = true;
    }
}