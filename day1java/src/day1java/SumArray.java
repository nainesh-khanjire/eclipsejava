package day1java;

import java.util.Scanner;

public class SumArray {

	// Write a Java program to sum values of an array.

	public static int[] ReadArrayElements()

	{
		Scanner sc = new Scanner(System.in);

		int[] numbers = new int[5];
		System.out.println("Enter the numbers >> ");

		for (int i = 0; i < 5; i++) 
		{
			numbers[i] = sc.nextInt();

		}

		return numbers;
	}

	public static int FindSumArrayElements(int[] numbers)
	{
		int sum = 0;
		
		for(int i=0; i<5;i++)
		{
			sum = sum + numbers[i];
			
		}
		return sum;
	}

	public static void main(String[] args) 
	{

		int[] numbers = ReadArrayElements();

		int sum = FindSumArrayElements(numbers);
		
		System.out.println("Sum of all array elements: "+sum);
		
	}
	}
