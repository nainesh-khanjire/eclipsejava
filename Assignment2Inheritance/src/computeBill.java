
public class computeBill extends readBill {

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
}
