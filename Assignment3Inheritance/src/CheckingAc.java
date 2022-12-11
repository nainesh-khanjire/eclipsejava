
public class CheckingAc extends BankAc {

	int fee = 200;

	void deductFee() {

		if (totalbalance < 1000) {
			totalbalance = totalbalance - 200;

		}
		System.out.println("Total balance in checking account has been updated");

		getBalance();

	}

}
