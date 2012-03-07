package prototype.utils;

import java.util.concurrent.BlockingQueue;
import java.util.*;

import prototype.datastore.*;

public class DataWriter extends Thread{

	//IDataServer dataServerStub;
	BlockingQueue<Map<String,byte[]>> dataBuffers;
	DataStore store;
	//Map<String,List<String>> userDataLookup;
	//Map<String,List<String>> dataUserRequests;

	public DataWriter(BlockingQueue<Map<String,byte[]>> buffers,DataStore st)
	{
//		userDataLookup = dataLookup;
	//	dataUserRequests = dataRequests;
		store = st;
		//dataServerStub = stub;
		dataBuffers = buffers;
	}

	public void run() 
	{
		while(true)
		{
			try
			{
				Map<String,byte[]> request = dataBuffers.take();
				Iterator<String> it = request.keySet().iterator();
				String data = it.next();
				byte[] buffer = request.get(data);	
				store.write(data, buffer);
				byte[] bytes = new byte[1];
				store.write(data+".marker", bytes);
		//		dataUserRequests.remove(data);
				/*List<String> users = dataUserRequests.get(data);
				Iterator<String> it1 = users.iterator();
				while(it1.hasNext())
				{
					String user = it1.next();
					it1.remove();
					List<String> datalist = userDataLookup.get(user);
					if(datalist == null)
						datalist = new ArrayList<String>();
					datalist.add(data);
					userDataLookup.put(user,datalist);
				}
				dataUserRequests.remove(data);
*/
			}catch(Exception e)
			{
				System.out.println("Exception in DataWriter Thread"+e);
			}
		}
	}
}