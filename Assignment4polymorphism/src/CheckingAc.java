import java.util.Scanner;

public class CheckingAc extends BankAc {

	
	int fee = 200;
	
	void deposit() {
		Scanner sc = new Scanner(System.in);

		System.out.println("ENTER FOLLOWING DETAILS FOR YOUR CHECKING ACCOUNT >> ");

		System.out.println("Enter your account number: ");
		accountnumber = sc.nextInt();

		System.out.println("Enter deposit amount: ");
		totalbalance = sc.nextDouble();

		System.out.println("Total balance after deposit has been updated");
		getBalance();

	}
	
	
	void deductFee() {
		
		if(totalbalance<1000) {
			totalbalance = totalbalance - 200;
			
		}
		System.out.println("Total balance in checking account has been updated");
		
		getBalance();
		
	}
	
}
