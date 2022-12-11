import java.util.Scanner;

public class ElecBill {

	int cnumber;
	String cname;
	double units;

	// assignment 2, electricity bill calculator

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

	public void ShowData() {
		System.out.println("The customer number is " + cnumber);
		System.out.println("the customer name is " + cname);
		System.out.println("The no. of units consumed are " + units);

	}

	public void ComputeBill() {
		double bill = 0;

		if (units <= 100) {
			bill = (100 * 1.20);

			System.out.println("your bill is " + bill);

		} else if (units <= 200) {
			bill = 100 * 1.20 + (units - 200) * 2;
			System.out.println("your total bill is " + bill);

		} else if (units <= 300) {
			bill = 100 * 1.20 + (units - 200) * 2 + (units - 300) * 3;
			System.out.println("your total bill is " + bill);

		} else
			bill = units * 5;
		System.out.println("your total bill is " + bill);

	}

	public static void main(String[] args) {

		ElecBill c1 = new ElecBill();

		c1.ReadData();
		c1.ShowData();
		c1.ComputeBill();

	}
}
