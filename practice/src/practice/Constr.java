package practice;

public class Constr {

	public static void main(String[] args) {
		System.out.println("new line");

		student s = new student();
		s.student(1, "nainu");

	}

}

class student {
	int id;
	String name;

	student() { // default constructor
		print(); // prints
	}

	void student(int id, String name) { // user defined constructor
		this.id = id;
		this.name = name;
		print();
	}

	void print() {
		System.out.println("my id is: " + id + " and name is: " + name);

	}

}