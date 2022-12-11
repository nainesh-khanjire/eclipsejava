package practice;

public class DefaultConstr {

	
	int id;
	int age;
	float salary;
	String dept;
	
	
	
	  DefaultConstr()
	  {
			this.id = 1008; // this is user defined constructor with own values
			this.age = 21;
			this.salary = 20000;
			this.dept = "prod";
		  
	  }
	void displayData() {
		System.out.println("Employee id is: " + id);
		System.out.println("employee age is: " + age + " years");
		System.out.println("employee salary is: " + salary + " rupees");
		System.out.println("employee department is: " + dept);
	}
	
	
	
	public static void main(String[] args) {
		
		DefaultConstr d1 = new DefaultConstr(); // paframeter less constructor
			// object will get allocated with default values 
			// this is done by DEFAULT CONSTRUCTOR
		
		
		d1.displayData(); // all values will be null by default
		 
		System.out.println(d1); // DefaultConstr@15db9742 >> hashcode for d1 object
		
		
		
		
		
	}

}
