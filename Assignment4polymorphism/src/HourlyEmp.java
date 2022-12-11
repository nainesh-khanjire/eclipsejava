import java.util.Scanner;

public class HourlyEmp extends Emp{

	int salary;
	
	
	HourlyEmp(){
		
		
	}
	void computegrosspay() {
		int tax=250;
		double rate=50;
		double hours;



		Scanner sc = new Scanner(System.in);
		System.out.println("enter hours worked >");
		hours = sc.nextInt();
		double grosspay = (hours*rate)-tax;

				print();
		
	}

	void computenetpay() {
		
		
	}
	public int getSalary() {
		return salary;
	}
	public void setSalary(int salary) {
		this.salary = salary;
	}


}
