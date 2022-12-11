package practice;

import java.util.Scanner;

public class Table {
	// 5. Java Program to Calculate Simple Interest.

	public static void main(String[] args) {
		double amount;
		double interest; // in percent
		int time; // in years
		double si;

		System.out.println("Enter following details to calculate simple interest");
		Scanner sc = new Scanner(System.in);

		System.out.println("Enter amount: ");
		amount = sc.nextDouble();

		System.out.println("Enter interest rate: ");
		interest = sc.nextDouble();

		System.out.println("Enter time period: ");
		time = sc.nextInt();

		si = (amount * interest * time) / 100;

		System.out.println("The total simple interest is: " + si);
	}

}
