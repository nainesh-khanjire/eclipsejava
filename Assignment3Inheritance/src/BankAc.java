import java.util.Scanner;

public class BankAc {

	int accountnumber;
	double totalbalance;

	BankAc() {

	}

	public BankAc(int accountnumber, double totalbalance) {
		super();
		this.accountnumber = accountnumber;
		this.totalbalance = totalbalance;
	}

	void deposit() {
		Scanner sc = new Scanner(System.in);

		System.out.println("Enter following details >> ");

		System.out.println("Enter your account number: ");
		accountnumber = sc.nextInt();

		System.out.println("Enter deposit amount: ");
		totalbalance = sc.nextDouble();

		System.out.println("Total balance after deposit has been updated");
		getBalance();

	}

	void withdraw() {
		double amount;

		Scanner sc = new Scanner(System.in);
		System.out.println("Enter amount to withdraw: ");
		amount = sc.nextDouble();

		totalbalance = totalbalance - amount;

		System.out.println("Total balance after withdrawl has been updated");

		getBalance();

	}

	void getBalance() {
		System.out.println("Your total balance is: " + totalbalance);

	}

}
