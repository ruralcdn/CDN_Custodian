import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;


public class CopyTest 
{

	public static void main(String[] args) throws IOException 
	{
	
		String source = "D:\\gaurav\\";
		String destination = "H:\\Labtest\\";
		File fs = new File(source+"855Mb.mkv");
		File fd = new File(destination+"\\JCopy\\855.mkv");
		jcopy(fs,fd);
		//ncopy(source+"10Mb.mp4",destination+"NCopy\\10Mb.mp4");
		//rcopy(source,destination+"Rcopy\\");
	}
	
	public static void jcopy(File src,File dest) throws IOException
	{
		long startTime = System.currentTimeMillis();
		if(!dest.exists())
		{
			dest.createNewFile();
		}
		FileChannel source = null;
		FileChannel destination = null;
		try
		{
			source = new FileInputStream(src).getChannel();
			destination = new FileOutputStream(dest).getChannel();
			destination.transferFrom(source,0,source.size());
		}
		catch(Exception ex)
		{
			System.out.println("Exception Occurred in jcopy");
		}
		finally
		{
			if (source != null)
			{
				source.close();
			}
			if (destination != null)
			{
				destination.close();
			}	
		}
		
		long endTime = System.currentTimeMillis();
		System.out.println("Java's IO Method takes "+(endTime-startTime)+"ms to copy ");
	}
	/*
	public static void ncopy(String src,String dest) throws IOException
	{
		System.out.println("Source:"+src);
		System.out.println("Destination:"+dest);
		try
		{
			long startTime = System.currentTimeMillis();
			Process p = Runtime.getRuntime().exec("copy "+src+" "+dest);
			int rc = p.waitFor();
			System.out.println("return code is:"+rc);
			long endTime = System.currentTimeMillis();
			System.out.println("System's Native Copy takes "+(endTime-startTime)+"ms to copy");
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
			System.out.println("Exception Occurred in ncopy");
		}
	}
	*/
	/*public static void rcopy(String src,String dest)
	{
		try
		{
			long startTime = System.currentTimeMillis();
			Process p = Runtime.getRuntime().exec("\"C:\\Program Files\\cwRsync\\bin\\rsync\" "+src+" "+dest);
			int rc = p.waitFor();
			System.out.println("return code is:"+rc);
			long endTime = System.currentTimeMillis();
			System.out.println("RSync takes "+(endTime-startTime)+"ms to copy");
		}
		catch(Exception ex)
		{
			System.out.println("Exception Occurred in rcopy");
		}
	}*/
}	