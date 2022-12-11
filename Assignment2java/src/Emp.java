import java.util.Scanner;

public class Emp {
	// assignment 2 , employee details, designation,dept , salary

	int empno;
	String empname;
	String designation;
	String dept;
	float salary;

	public void displayEmpData() {
		System.out.println(empno);
		System.out.println(empname);
		System.out.println(designation);
		System.out.println(dept);
		System.out.println(salary);

	}

	public void readEmpData() {
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter Employee details >> ");

		System.out.println("Enter Employee number: ");
		empno = sc.nextInt();

		System.out.println("Enter Employee name: ");
		empname = sc.next();

		System.out.println("Enter designation of Employee: ");
		designation = sc.next();

		System.out.println("Enter Employee department: ");
		dept = sc.next();

		System.out.println("Enter Employee salary: ");
		salary = sc.nextFloat();
		sc.close();
	}

	public static void main(String[] args) {

		Emp e1 = new Emp();

		e1.readEmpData();
		e1.displayEmpData();

	}


}