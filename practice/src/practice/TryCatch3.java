package practice;

public class TryCatch3 {

	public static void main(String[] args) {

		try {
			int s = 50 / 0;
			System.out.println("rest of the code"); // this line will not execute
		} catch (ArithmeticException e) {
			System.out.println(e);

		}

	}

}
