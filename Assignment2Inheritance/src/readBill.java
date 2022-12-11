import java.util.Scanner;

public class readBill extends ElectricBill {


	public void ReadData() {
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter consumer details >>");

		System.out.println("Enter consumer number: ");
		cnumber = sc.nextInt();

		System.out.println("Enter consumer name: ");
		cname = sc.next();

		System.out.println("Enter units consumed: ");
		units = sc.nextDouble();

		sc.close();
	}

	
}
