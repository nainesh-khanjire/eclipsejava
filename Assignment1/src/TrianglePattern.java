import java.util.Scanner;

public class TrianglePattern {

	public static void main(String[] args) {

		Scanner sc = new Scanner(System.in);
		System.out.println("Enter the number of rows you want to print: ");
		int rows = sc.nextInt();

		for (int i = 0; i < rows; i++) { // i is for printing rows of the given input

			for (int j = 0; j <= i; j++) { // j is for  printing coloumns
				
				System.out.print(" * "); // space between two stars
			}
			System.out.println(); // this takes cursor to new line

		
	}
}
}