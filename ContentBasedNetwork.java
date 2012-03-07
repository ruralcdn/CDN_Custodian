import java.util.Scanner;
import prototype.cache.CacheServer;
import prototype.custodian.Logger;
import prototype.dataserver.DataServer;
import prototype.dbserver.DBServer;
import prototype.rendezvous.RendezvousServer;
import prototype.rootserver.RootServer;
import prototype.serviceinstance.ServiceInstance;
import prototype.user.User1;
import prototype.userregistrar.CustodianLogger;

public class ContentBasedNetwork
{

	public static void main(String[] args) throws Exception
	{
		System.out.println("Choose Entity:" +
				"\n1.Custodian\n" +
				"2.User\n" +
				"3.LookupServer\n"+
				"4.DataServer\n"+
				"5.Service Instance\n"+
				"6.Root Server\n"+
				"7.User Registrar\n"+
				"8.CacheServer\n"+
				"9.DBServer\n");

		Scanner in = new Scanner(System.in);
		boolean done = false;

		while(!done)
		{
			int input = in.nextInt();	
			done = false;
			switch(input)
			{
			case 1: Logger.main(null);done = true;break;
			case 2: User1.main(null);done = true;break;
			case 3: RendezvousServer.main(null);done = true;break;
			case 4: DataServer.main(null);done = true;break;
			case 5: ServiceInstance.main(null);done = true;break;
			case 6: RootServer.main(null);done = true;break;
			case 7: CustodianLogger.main(null);done = true;break;
			case 8:	CacheServer.main(null);done = true;break;
			case 9:	DBServer.main(null);done = true;break;
			default:System.out.println("Please enter a number 1-9");
			}
		}
	//Logger.main(null);
	//CacheServer.main(null);
	}
}
