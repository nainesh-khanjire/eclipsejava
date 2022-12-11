
public class EmployeeMain {

	public static void main(String[] args) {
		 // Employee ,salaried, hourly

		SalariedEmployee s = new SalariedEmployee();

		s.computeGrossPay();
		s.computeGrossPay();
		s.setSalary(15000);
		s.getSalary();
		
		System.out.println(s);

		HourlyEmployee h = new HourlyEmployee(1001,"nainesh","khanjire");
		h.setFirstname("nainesh");
		h.setLastname ("khanjire");
		h.setSSN(1001);
		h.computeGrossPay();
		h.computeNetPay();
		h.setSalary(20000);
		h.getSalary();

		System.out.println(h);

	}

}
