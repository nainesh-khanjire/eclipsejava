import java.util.Scanner;

public class Even {
public static void main(String[] args) {
	// program to check if the numer is even or odd 
	Scanner sc = new Scanner(System.in);
	System.out.println("Enter a number to check if its even or odd: ");
	 int number = sc.nextInt();
	 
	 if (number%2==0)
			 {
		 System.out.println(number+" is an even number ");
	 }else {
		 System.out.println(number+" is odd number");
	 }
}
}
