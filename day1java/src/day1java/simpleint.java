package day1java;

import java.util.Scanner;

public class simpleint {

	// 5.	Java Program to Calculate Simple Interest
	
	
	
	public static void main(String[] args) {
		
		float principle, interest, time, si, outstanding;
		
		Scanner sc = new Scanner(System.in);
		
			System.out.println("Enter the principle amount (in rupees):");
			principle = sc.nextFloat();
	
			System.out.println("Enter the interest rate (in %):");
			interest = sc.nextFloat();
			
			System.out.println("Enter the time period (in years):");
			time = sc.nextFloat();
			
		si = principle * interest * time / 100;
		outstanding = si + principle;
		
		System.out.println("The simple interest is (in rupees)  : " + si);
		System.out.println("your total amount (including interest) is (in rupees) :" + outstanding);
		
		
		
	}
}
