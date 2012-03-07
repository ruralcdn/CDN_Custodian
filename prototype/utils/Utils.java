package prototype.utils;

import java.util.ArrayList;
import java.util.List;

public class Utils{


	public static Object toObject(byte[] bytes){
		Object object = null;
		try{
			object = new java.io.ObjectInputStream(new
					java.io.ByteArrayInputStream(bytes)).readObject();
		}catch(java.io.IOException ioe){

		}catch(java.lang.ClassNotFoundException cnfe){

		}
		return object;
	}

	public static List<String> parse(String input)
	{
		String trimmedInput = input.trim();
		String list = trimmedInput.substring(1,trimmedInput.length()-1);
		List<String> output = new ArrayList<String>();
		if(list.length()>0)
		{
			String[] out = list.split(",");
			for(int i = 0;i < out.length;i++)
			{
				output.add(out[i].trim());
			}
		}
		return output;
	}
	
	public static byte[] toBytes(Object object){
		java.io.ByteArrayOutputStream baos = new
		java.io.ByteArrayOutputStream();
		try{
			java.io.ObjectOutputStream oos = new
			java.io.ObjectOutputStream(baos);
			oos.writeObject(object);
		}catch(java.io.IOException ioe){

		}
		return baos.toByteArray();
	}

	public static int byteArrayToInt(byte[] b, int offset) {
		int value = 0;
		for (int i = 0; i < 4; i++) {
			int shift = (4 - 1 - i) * 8;
			value += (b[i + offset] & 0x000000FF) << shift;
		}
		return value;
	}


}
