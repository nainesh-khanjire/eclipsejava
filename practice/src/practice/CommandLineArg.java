package practice;

public class CommandLineArg {

	public static void main(String[] args) { // data type of args is String
//		int a = args[0]; // trying to store String into integer
//		int b = args[1]; javac error

//		int a = Integer.parseInt(args[0]);
//		int b = Integer.parseInt(args[1]);
//
//		int c = a + b;

		int a = Integer.parseInt(args[0]); // javac error, convert string to int
		float b = Float.parseFloat(args[1]); // convert string to float
		double c = Double.parseDouble(args[2]); // convert String to double

		System.out.println("a is " + a + ", b is " + b + " and c is " + c);

	}
}
