package practice;

public class TryCatch4 {
	public static void main(String[] args) {
		int a = 50;
		int b = 0;

		try {

			int c = a / b; // this will throw an exception
		} catch (Exception x) {

			System.out.println("Displaying the error type: " + x);
			System.out.println("can be fixed by modifying the variables, " + a / (b + 1));

		}

	}
}
