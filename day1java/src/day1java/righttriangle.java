package day1java;

import java.util.Scanner;

// 6.	Java Program to Print Right Triangle Star Pattern
public class righttriangle {

	public static void main(String[] args) {
	
		int i, j, rows;
		
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter the number of rows that you want to print triangle of:");
		rows = sc.nextInt();
		
		for (i=0;i<rows;i++) 
		{
			for (j=0;j<=i;j++)
			{
				System.out.print("*"); 		
			}
			System.out.println(); 				
	}
	}
	}
