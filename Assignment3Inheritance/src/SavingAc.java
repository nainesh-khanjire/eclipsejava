
public class SavingAc extends BankAc {

	double interestrate = 0.02D;

	void addInterest() {

		if (totalbalance > 1000) {

			totalbalance = totalbalance + (totalbalance * 0.02);
		} else {
			System.out.println("interest is not applicable for this balance!");
		}

		System.out.println("Total balance with interest has been updated");

		getBalance();
	}

}
