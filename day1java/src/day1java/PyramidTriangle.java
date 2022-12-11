package day1java;

import java.util.Scanner;

public class PyramidTriangle {

	// 7.	Java Program to Print Pyramid Number Pattern
	public static void main(String[] args) {
		

		int rows, k = 0;
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter the number of rows that you want to print triangle of:");
		rows = sc.nextInt();
		
		

	    for (int i = 1; i<=rows; ++i, k = 0) {
	      for (int space = 1; space <= rows - i; ++space) {
	        System.out.print("  ");
	      }

	      while (k != 2 * i - 1) {
	        System.out.print("* ");
	        ++k;
	      }

	      System.out.println();
	    }
	  }
}
