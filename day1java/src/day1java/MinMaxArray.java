package day1java;

import java.util.Scanner;

public class MinMaxArray {

	// Write a Java program to find the maximum and minimum value of an array.

	public static int[] ReadArray() {
		
		Scanner sc = new Scanner(System.in);
		int[] numbers = new int[5];

		System.out.println("Enter the numbers >> ");

		for (int i = 0; i < 5; i++) {
			numbers[i] = sc.nextInt();

		}

		return numbers;
	}

	public static void main(String[] args) {

		int[] numbers = ReadArray(); 

		System.out.println("The minimum value from given numbers is: "+min);
		System.out.println("The maximum value from given numbers is: "+max);
	}
}
