
package practice;

import java.util.Scanner;

public class ScannerSc {

	public static void main(String[] args) {

		int a;
		float b;
		String c;

		Scanner sc = new Scanner(System.in);
		System.out.println("enter a=");

		a = sc.nextInt();

		System.out.println("enter b=");
		b = sc.nextFloat();

		System.out.println("enter c=");
		c = sc.next();

		

		System.out.println("a=" + a);
		System.out.println("b=" + b);
		System.out.println("c=" + c);
		
		System.out.printf("\n%10d %5.3f %3s", a,b,c);
		sc.close();
	}

}
