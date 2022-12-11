package day1java;

public class HelloName {

	public static void HelloName(String name)
	{
	
		System.out.println("inside the helloname");
		System.out.println("Hello " + name);
	}
	
	public static void main(String[] args) {
		
		String name = "nainesh";
		HelloName(name);
		
		System.out.println("inside main");
		 	
	}
}

