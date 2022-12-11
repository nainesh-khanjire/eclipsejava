package day1java;

import java.util.Scanner;

public class TableOfNumbers {
	
// 8.	Write a java program  to  print Table of given  Number 
	
	public static void main(String[] args) {
		

	
	Scanner sc = new Scanner(System.in);
	System.out.println("enter the number that you want table of: ");
	int num=sc.nextInt();
	
	for (int i=1;i<=20;i++)
	{
		System.out.println(num+"*"+i+"="+num*i);
	}
}
}