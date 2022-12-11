package practice;

import java.util.Scanner;

public class Emplo {

	int id;
	int age;
	float salary;
	String dept;

	Emplo() {
//		this.id=101;
//		this.age=20;
//		this.salary=5000;
//		this.dept= "doc" ;
		this(0, 0, 0, "DEFAULT"); // default constructor

	}

	Emplo(int id, int age, float salary, String dept) {

		this.id = id;
		this.age = age;
		this.salary = salary;
		this.dept = dept;

	}

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

		
		Emplo e2 = new Emplo(); // e2 object created by passing values/ passing default values
		e2.displayData();
		
		Emplo e1 = new Emplo();
		e1.readData(); // taking input from user
		e1.displayData();

		

	}

}
