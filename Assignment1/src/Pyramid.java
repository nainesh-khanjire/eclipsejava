public class Pyramid {

	public static void main(String[] args) {
		// pattern(5);
		pattern1(6);
	}

	static void pattern(int n) {
	   	for (int row = 0; row <= n; row++) {
			for (int colm = 0; colm <= row; colm++) {

				System.out.print("* ");
			}

			System.out.println();
		}
	}

	static void pattern1(int m) {

		for (int row = 0; row <= m; row++) {

			for (int colm = 0; colm < row; colm++) {
				System.out.print(" * ");
			
			}
			System.out.println();
		}

	}

}
