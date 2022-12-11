package day1java;

import java.util.Scanner;

// 2.	Write a program to find SUM of digits in given number
	public class sumofdigits {

		
		public static void main(String[] args) {
			
			
			long number, sum=0 ,digit;
			
			Scanner sc = new Scanner(System.in);
			
			System.out.println("Enter the number :");
			
			number = sc.nextLong();
			
			while(number>0) 
			{
			digit = number % 10;
			sum = sum + digit;
			number = number/10;
			
			}
		
			System.out.println("The sum of digits is: " + sum);
			}
			}
