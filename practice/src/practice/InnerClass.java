package practice;

public class InnerClass { // outside(standalone) classes cannot be static

	static class A { // this class itself is dependent on the outer class
		// this class A doesnt really have any objects, works only if it was independent
		// class
		// or make this class A as static
		// by making this class static, object is not required
		String name; // make this static and it will get assigned 

		public A(String name) {
			super();
			this.name = name;
		}
	}

	public static void main(String[] args) {

		A a = new A("NAINESH"); // no errors when class A made static or kept independent of any other class
		A b = new A("SNOOP");

		System.out.println(a.name);

		System.out.println(b.name);

	}
}
