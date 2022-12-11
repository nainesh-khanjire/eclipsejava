package day1java;

import java.util.Scanner;

public class oddoreven {

	// 4.	Java Program to Check Whether a Number is Even or Odd
public static void main(String[] args) {
	
	int a;
	
	Scanner sc = new Scanner(System.in);
		System.out.println("Enter the number to find out whether its odd or even:");
		a= sc.nextInt();
	
		
	
		if (a%2 == 0)
		{
			System.out.println("The given Number is even");
			
			
		} else
			
		{
			
			System.out.println("The given Number is odd");
		}
		
}
	
	
	
	
	
}
