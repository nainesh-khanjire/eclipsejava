package day1java;

import java.util.Scanner;

public class LargestOfThree {

	public static void main(String[] args) {
		
		int a,b,c,comp,largest;
		Scanner sc = new Scanner(System.in);
		
		System.out.println("Enter the first number: ");
		 a = sc.nextInt(); 
		
		System.out.println("Enter the second number: ");
		 b = sc.nextInt(); 
		
		System.out.println("Enter the third number: ");
		 c = sc.nextInt(); 
		
		comp =a > b ? a : b; 
		largest= c > comp ? c : comp;  
		
		System.out.println("The largest of above given numbers is: "+largest);
	}
}
