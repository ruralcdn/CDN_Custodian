package DBSync;

public class DBSync {
	
	SyncServer syncSer ;
	LocalUpdate localUpdate ;
	public DBSync(int port)
	{
		syncSer	= new SyncServer(6789);
		localUpdate = new LocalUpdate();
		syncSer.start();
		localUpdate.start();
	}
}
