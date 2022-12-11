
public class BankAcMain {

	public static void main(String[] args) {
		// Write java program to implement POLYMORPHISM with following example

		SavingAc s = new SavingAc();
		System.out.println("SAVING ACCOUNT >>");
		s.deposit();
		s.withdraw();
		s.addInterest();

		CheckingAc c = new CheckingAc();
		System.out.println("CHECKING ACCOUNT >>");
		c.deposit();
		c.withdraw();
		c.deductFee();
	}

}
