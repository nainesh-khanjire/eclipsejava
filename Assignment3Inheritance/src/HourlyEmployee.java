import java.util.Scanner;

class HourlyEmployee extends Employee 
{

	double totalsal;

	public HourlyEmployee(int i, String string, String string2) 
	{
		this.totalsal =1;
	}
		
	

public HourlyEmployee() {
		
		this.totalsal= 0;

	void computeGrossPay() {
		Scanner sc = new Scanner(System.in);

		int salaryperhr = 400;
		int workhours;
		System.out.println("enter hours worked: ");
		workhours = sc.nextInt();
		totalsal = workhours * salaryperhr;
		System.out.println("Gross salary of employee is : ");
		print();

	}

	void computeNetPay() {
		double tax = 0.05D;
		totalsal = totalsal - (totalsal * tax);
		System.out.println("net salary of employee is : ");
		print();
	}

	public void setSalary(int salary) {
		this.totalsal = 15000;
	}

	public double getSalary() {
		return totalsal;
	}

	void print() {
		System.out.println(totalsal);
	}

}
	
