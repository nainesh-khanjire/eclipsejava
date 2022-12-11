package practice;

public class Wrapper {

	public static void main(String[] args) {
		int i = 3;
		Integer I = 45;

		Integer a = 4; // Integer is a FINAL class
		Integer b = 5; 
		swap(a, b);
		System.out.println("a=" + a + " b=" + b); // this will not swap, java is pass by value

		final int x = 1; // final keyword variables must be initialised
		// x= 5; 	     // final variable cannot be assigned or changed

	}

	static void swap(Integer a, Integer b) {

		int temp = a;
		a = b;
		b = temp;
	}

	
	
}