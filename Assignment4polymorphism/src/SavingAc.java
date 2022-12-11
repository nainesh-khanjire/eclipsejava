import java.util.Scanner;

public class SavingAc extends BankAc 
{

	double interestrate;

	
	void deposit() {
		Scanner sc = new Scanner(System.in);

		System.out.println("ENTER FOLLOWING DETAILS FOR SAVING ACOUNT >> ");

		System.out.println("Enter your account number: ");
		accountnumber = sc.nextInt();

		System.out.println("Enter deposit amount: ");
		totalbalance = sc.nextDouble();
		
		System.out.println("Enter interest rate(in %): ");
		interestrate = sc.nextDouble();

		System.out.println("Total balance after deposit has been updated");
		getBalance();

	}
	void addInterest() {
		

		if (totalbalance > 1000) {

			totalbalance = totalbalance + ((totalbalance * interestrate)/100);
		}
		else
		{
			System.out.println("interest is not applicable for this balance!");
	    }

		System.out.println("Total balance with interest has been updated");

		getBalance();
	}

}

