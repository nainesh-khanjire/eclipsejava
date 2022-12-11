package day1java;

import java.util.Scanner;

// 1. Write a program to find SUM of a given digit
public class findsum {

public static void main(String[] args) 
	{
	
	int a, b;
	int sum = 0;
	
	Scanner sc = new Scanner(System.in);
	
	System.out.println("Enter the first number");
	a= sc.nextInt();
	b= sc.nextInt();
	sum = sum(a,b);
	
	System.out.println("The sum of given numbers is : " + sum);
	
	}
	
	// sum function
	public static int sum(int a, int b)
	
	{
	int sum = a + b;
	return sum;
		
	}
	


}
