import java.util.Scanner;

public class Accounts {

	// assignment 2, accountno, accountname, amount

	int accountno;
	String accountname;
	double amount;

	public void ReadData() {
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter the following details >>");

		System.out.println("Enter the Account number: ");
		accountno = sc.nextInt();

		System.out.println("Enter Account-holder name: ");
		accountname = sc.next();

		System.out.println("Enter the Amount: ");
		amount = sc.nextDouble();

		sc.close();

	}

	public void Displaydata() {
		System.out.println(accountno);
		System.out.println(accountname);
		System.out.println(amount);
	}

	public void WithdrawAmount(int withdraw) {
		if (amount >= 1000)

		{
			amount = amount - withdraw;
			System.out.println("The amount after withdrawl is : " + amount);
		} else
			System.out.println("Withdraw failed :/ Low Balance !!!");

	}

	public void DepositAmount(double depo) {
		depo = amount + depo;
		System.out.println("The amount after deposit is: " + depo);

	}

	public void Interest(double intrst) {
		if (amount >= 1000) {
			amount = amount + (amount * intrst);
			System.out.println("The amount after applying interest is: " + amount);
		} else
			System.out.println("Interest not applicable for this balance !!!");

	}

	public static void main(String[] args) {

		Accounts a1 = new Accounts();

		a1.ReadData();
		a1.Displaydata();
		a1.WithdrawAmount(300);
		a1.DepositAmount(10000);
		a1.Interest(10);

	}
}
