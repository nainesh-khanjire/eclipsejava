import java.util.Scanner;

public class Employee {

	int empno;
	String empname;
	String dept;
	String designation;

	double salary;

	
}
		public class readData extends Employee {

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

		}

		public class displayData extends Employee {


			void displayData()
			{
				System.out.println(empno);
				System.out.println(empname);
				System.out.println(designation);
				System.out.println(dept);
				System.out.println(salary);
			
		}
		
//void displayData()
//{
//	System.out.println(empno);
//	System.out.println(empname);
//	System.out.println(designation);
//	System.out.println(dept);
//	System.out.println(salary);
//}
