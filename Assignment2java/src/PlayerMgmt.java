

public class PlayerMgmt {


	
	Player[] createPlayer()
	{
		
		Player[] p1 = new Player[1];
		
		for(int i=0;i<p1.length; i++)
		{
			p1[i] = new Player();
			p1[i].ReadPlayer();
		}
		return p1;
		
	}
	
	void DisplayData(Player[] p1)
	{
		for(int i=0;i<p1.length;i++)
		{
		p1[i].DisplayData();
		}
		}

	
	
}
