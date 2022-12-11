import java.util.Scanner;

public class Player {



	String name;
	int age;
	String country;
	int totalrun;

	public void ReadPlayer() {
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter the follwing details >> ");
 
		System.out.println("Enter player name: ");
		name = sc.next();

		System.out.println("Enter age of player: ");
		age = sc.nextInt();

		System.out.println("Enter country of player: ");
		country = sc.next();

		System.out.println("Enter total runs: ");
		totalrun = sc.nextInt();

		
	}

	public void DisplayData() 
	{

	System.out.println("Name of player is "+name);
	System.out.println("Age of player is "+age);
	System.out.println("Country of player is "+country);
	System.out.println("Total runs made by player "+totalrun);
	
	}


}
