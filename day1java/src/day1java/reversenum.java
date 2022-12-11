package day1java;

import java.util.Scanner;

public class reversenum {

	// 3.	Write a java program to reverse the given number
	
	public static void main(String[] args) {
		
		int a; int reverse = 0;
		
		Scanner sc = new Scanner(System.in);
			System.out.println("Enter the number :");
		
		a= sc.nextInt();
		while(a!= 0)
			
		{
		int remainder = a % 10;
		reverse = reverse * 10 + remainder;
		a = a /10;
		}		
			System.out.println("The reverse of given number is : " + reverse);
	 
				
	}
	
}
