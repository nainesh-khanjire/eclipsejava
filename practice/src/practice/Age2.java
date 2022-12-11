package practice;
import java.util.Scanner;

public class Age2 {

	
	public static void main(String...args) {
		
		int age[] = new int[10];
		Scanner sc = new Scanner(System.in);
		for(int i=0;i<=age.length-1;i++) {
			System.out.println("Enter array "+i+" element: ");
			age[i] = sc.nextInt();
	
		}
		
	for (int i=0;i<=age.length-1;i++) {
		
	System.out.println("Entered elements are: "+i);
	}
	
	}
}
