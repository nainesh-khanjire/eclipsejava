package practice;

import java.util.Scanner;

public class Employment {

	int id;
	int age;
	float salary;
	String dept;

	void readData() {
		System.out.println("Enetr the following details >> ");
		Scanner sc = new Scanner(System.in);

		System.out.println("Enter employee id: ");
		id = sc.nextInt();

		System.out.println("Enter employee age: ");
		age = sc.nextInt();

		System.out.println("Enter employee salary: ");
		salary = sc.nextFloat();

		System.out.println("Enter employee department: ");
		dept = sc.next();

		
	}

	void displayData() {
		System.out.println("Employee id is: " + id);
		System.out.println("employee age is: " + age + " years");
		System.out.println("employee salary is: " + salary + " rupees");
		System.out.println("employee department is: " + dept);
	}

	public static void main(String[] args) {

		// e1 is reference variable, local variable
		// if e1 = null; no javac error will occur
		// this is throw NPE error, null pointer exception error

		Employment e1 = new Employment();
		e1.readData();
		e1.displayData();

		Employment e2 = new Employment();
		e2.readData();
		e2.displayData();
		
		Employment e3 = e1; // e3 points/referes to the e1 locatio,  called shallow copy
//		
//		System.out.println(e1); // Employment@55f96302 hashcode	of object e1
//		System.out.println(e2); // Employment@3d4eac69 hashcode of object e2
//		System.out.println(e3);	// Employment@55f96302 hashcode of e3 similar to e1
	}
}
