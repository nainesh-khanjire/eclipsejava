
public class BankAcMain {

	public static void main(String[] args) {
		// Write java program to implement INHERITANCE with following example

		SavingAc s = new SavingAc();

		s.deposit();
		s.withdraw();
		s.addInterest();

		CheckingAc c = new CheckingAc();

		c.deposit();
		c.withdraw();
		c.deductFee();
	}

}
