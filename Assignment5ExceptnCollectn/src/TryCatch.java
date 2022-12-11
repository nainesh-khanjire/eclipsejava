
public class TryCatch {

	public static void main(String[] args) 
	
	{
		/*
		 * 1) Write a program to demonstrate the use of try, catch, finally throw and
		 * throws keywords and demonstrate the following points in the program. 
		 * a) Multiple catch blocks. 
		 * b) try-catch-finally combination. 
		 * c) try-finally combination. 
		 * d) Nested try blocks
		 */

		System.out.println("a) multiple catch blocks >>");
		// a) multiple catch blocks

		try {
			int a = 5;
			int b = 0;

			int c = a / b; // this creates error

		} catch (ArithmeticException v) {
			System.out.println("Arithmetic exception displayed: " + v); // displays error

		} catch (ArrayIndexOutOfBoundsException i) {
			System.out.println("array exception displayed :" + i);

		} catch (Exception x) {
			System.out.println("Exception displayed: " + x);
		}
		System.out.println("rest of the code"); // this print stmt gets executed

		System.out.println("\n\nb) try-catch-finally combination >>");
		// b) try-catch-finally combination
		try {
			int a = 50;
			int d = 0;
			// int c1 = a / d; // error expected, uncomment before running b)

		} catch (NullPointerException a) { // used wrong exception type, creates error
			System.out.println(a);
		} finally {
			System.out.println("this finally block will get executed regardless of the errors");
		}
		System.out.println("this is rest of the code"); // default print stmt gets printeds

		System.out.println("\n\nc) try-finally combination >>");

		// c) try-finally combination
		try {
			int a = 40;
			int e = 0;
			// int c2 = a / e; // error expected , uncomment before running this block

		} finally {

			System.out.println("this is try finally block that will get executed");
		}

		System.out.println("\n\nd) try-nested block >>");

		// d) Nested try blocks

		try {
			int p = 30;
			int q = 0;
			int r = 0;
			int s = 0;

			int t = p / q; // error 1
			try {
				int u = p / r; // error 2
				try {
					int y = p / s; // error 3

				} catch (ArithmeticException a) {
					System.out.println(a);

				}
			} catch (ArithmeticException n) {
				System.out.println(n);

			}
		} catch (ArithmeticException m) {

			System.out.println(m);

		}}
	}